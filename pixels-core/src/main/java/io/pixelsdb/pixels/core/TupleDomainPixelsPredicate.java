/*
 * Copyright 2017-2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core;

import io.pixelsdb.pixels.core.stats.BooleanColumnStats;
import io.pixelsdb.pixels.core.stats.ColumnStats;
import io.pixelsdb.pixels.core.stats.DoubleColumnStats;
import io.pixelsdb.pixels.core.stats.IntegerColumnStats;
import io.pixelsdb.pixels.core.stats.RangeStats;
import io.pixelsdb.pixels.core.stats.StringColumnStats;
import io.pixelsdb.pixels.core.stats.TimestampColumnStats;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * pixels
 *
 * @author guodong
 */
public class TupleDomainPixelsPredicate<C>
        implements PixelsPredicate
{
    private final TupleDomain<C> predicate;
    public final List<ColumnReference<C>> columnReferences;

    public TupleDomainPixelsPredicate(TupleDomain<C> predicate, List<ColumnReference<C>> columnReferences)
    {
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.columnReferences = ImmutableList.copyOf(requireNonNull(columnReferences, "column references is null"));
    }

    /**
     * Check if predicate matches statistics
     *
     * @param numberOfRows            number of rows
     * @param statisticsByColumnIndex statistics map. key: column index in user specified schema,
     *                                value: column statistic
     */
    @Override
    public boolean matches(long numberOfRows, Map<Integer, ColumnStats> statisticsByColumnIndex)
    {
        Optional<Map<C, Domain>> optionalDomains = predicate.getDomains();
        if (!optionalDomains.isPresent())
        {
            // predicate is none, so skip the reading content
            return false;
        }
        Map<C, Domain> domains = optionalDomains.get();

        for (ColumnReference<C> columnReference : columnReferences)
        {
            Domain predicateDomain = domains.get(columnReference.getColumn());
            if (predicateDomain == null)
            {
                // no predicate on this column, so continue
                continue;
            }
            ColumnStats columnStats = statisticsByColumnIndex.get(columnReference.getOrdinal());
            if (columnStats == null)
            {
                // no column statistics, so continue
                continue;
            }

            if (!domainOverlaps(columnReference, predicateDomain, numberOfRows, columnStats))
            {
                return false;
            }
        }

        return true;
    }

    private boolean domainOverlaps(ColumnReference<C> columnReference, Domain predicateDomain,
                                   long numberOfRows, ColumnStats columnStats)
    {
        Domain columnDomain = getDomain(columnReference.getType(), numberOfRows, columnStats);
        if (!columnDomain.overlaps(predicateDomain))
        {
            return false;
        }

        if (predicateDomain.isNullAllowed() && columnDomain.isNullAllowed())
        {
            return true;
        }

        Optional<Collection<Object>> discreteValues = getDiscreteValues(predicateDomain.getValues());
        if (!discreteValues.isPresent())
        {
            return true;
        }

        return true;
    }

    private Optional<Collection<Object>> getDiscreteValues(ValueSet valueSet)
    {
        return valueSet.getValuesProcessor().transform(
                ranges -> {
                    ImmutableList.Builder<Object> discreteValues = ImmutableList.builder();
                    for (Range range : ranges.getOrderedRanges())
                    {
                        if (!range.isSingleValue())
                        {
                            return Optional.empty();
                        }
                        discreteValues.add(range.getSingleValue());
                    }
                    return Optional.of(discreteValues.build());
                },
                discreteValues -> Optional.of(discreteValues.getValues()),
                allOrNone -> allOrNone.isAll() ? Optional.empty() : Optional.of(ImmutableList.of()));
    }

    private Domain getDomain(Type type, long rowCount, ColumnStats columnStats)
    {
        if (rowCount == 0)
        {
            return Domain.none(type);
        }

        if (columnStats == null)
        {
            return Domain.all(type);
        }

        if (columnStats.getNumberOfValues() == 0)
        {
            return Domain.onlyNull(type);
        }

        boolean hasNullValue = columnStats.getNumberOfValues() != rowCount;

        if (type.getJavaType() == boolean.class)
        {
            BooleanColumnStats booleanColumnStats = (BooleanColumnStats) columnStats;
            boolean hasTrueValues = booleanColumnStats.getTrueCount() != 0;
            boolean hasFalseValues = booleanColumnStats.getFalseCount() != 0;
            if (hasTrueValues && hasFalseValues)
            {
                return Domain.all(BOOLEAN);
            }
            if (hasTrueValues)
            {
                return Domain.create(ValueSet.of(BOOLEAN, true), hasNullValue);
            }
            if (hasFalseValues)
            {
                return Domain.create(ValueSet.of(BOOLEAN, false), hasNullValue);
            }
        }
        else if (isCharType(type))
        {
            return createDomain(type, hasNullValue, (StringColumnStats) columnStats);
        }
        else if (isVarcharType(type))
        {
            return createDomain(type, hasNullValue, (StringColumnStats) columnStats);
        }
        else if (type instanceof TimestampType)
        {
            return createDomain(type, hasNullValue, (TimestampColumnStats) columnStats);
        }
        else if (type.getJavaType() == long.class)
        {
            return createDomain(type, hasNullValue, (IntegerColumnStats) columnStats);
        }
        else if (type.getJavaType() == double.class)
        {
            return createDomain(type, hasNullValue, (DoubleColumnStats) columnStats);
        }
        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    private <T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue,
                                                          RangeStats<T> rangeStats)
    {
        if (type instanceof VarcharType || type instanceof CharType)
        {
            return createDomain(type, hasNullValue, rangeStats, value -> value);
        }
        return createDomain(type, hasNullValue, rangeStats, value -> value);
    }

    private <F, T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue,
                                                             RangeStats<F> rangeStats,
                                                             Function<F, T> function)
    {
        F min = rangeStats.getMinimum();
        F max = rangeStats.getMaximum();

        if (min != null && max != null)
        {
            return Domain.create(
                    ValueSet.ofRanges(
                            Range.range(type, function.apply(min), true, function.apply(max), true)),
                    hasNullValue);
        }
        if (max != null)
        {
            return Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, function.apply(max))), hasNullValue);
        }
        if (min != null)
        {
            return Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, function.apply(min))), hasNullValue);
        }

        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    public static class ColumnReference<C>
    {
        private final C column;
        private final int ordinal;
        private final Type type;

        public ColumnReference(C column, int ordinal, Type type)
        {
            this.column = requireNonNull(column, "column is null");
            checkArgument(ordinal >= 0, "ordinal is negative");
            this.ordinal = ordinal;
            this.type = requireNonNull(type, "type is null");
        }

        public C getColumn()
        {
            return column;
        }

        public int getOrdinal()
        {
            return ordinal;
        }

        public Type getType()
        {
            return type;
        }
    }
}
