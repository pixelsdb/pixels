/*
 * Copyright 2022 PixelsDB.
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
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.executor.predicate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;
import com.google.common.reflect.TypeToken;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.stats.RangeStats;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.utils.Decimal;
import io.pixelsdb.pixels.core.vector.*;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Created at: 07/04/2022
 * Author: hank
 */
@JSONType(includes = {"columnName", "columnType", "filterJson"})
public class ColumnFilter<T extends Comparable<T>>
{
    @JSONField(name = "columnName", ordinal = 0)
    private final String columnName;
    @JSONField(name = "columnType", ordinal = 1)
    private final TypeDescription.Category columnType;
    // storing filterJson as a field of this class to reduce json serialization overhead.
    @JSONField(name = "filterJson", ordinal = 2)
    private String filterJson = null;
    // TODO: automatic initialization of this.filter is not yet implemented for gson.
    private final Filter<T> filter;
    private final Set<T> includes;
    private final Set<T> excludes;

    public ColumnFilter(String columnName, TypeDescription.Category columnType, Filter<T> filter)
    {
        this.columnName = columnName;
        this.columnType = columnType;
        this.filter = filter;
        this.includes = new HashSet<>();
        this.excludes = new HashSet<>();
        for (Bound<T> discrete : this.filter.discreteValues)
        {
            if (discrete.type == Bound.Type.INCLUDED)
            {
                includes.add(discrete.value);
            }
            else
            {
                excludes.add(discrete.value);
            }
        }
    }

    /**
     * This constructor is mainly used by fastjson.
     * @param columnName
     * @param columnType
     * @param filterJson
     */
    @JSONCreator
    public ColumnFilter(String columnName, TypeDescription.Category columnType, String filterJson)
    {
        this.columnName = columnName;
        this.columnType = columnType;
        this.filterJson = filterJson;
        Class<?> columnJavaType = columnType.getInternalJavaType();
        Type filterType;
        if (columnJavaType == byte.class)
        {
            filterType = new TypeToken<Filter<Byte>>(){}.getType();
        }
        else if (columnJavaType == int.class)
        {
            filterType = new TypeToken<Filter<Integer>>(){}.getType();
        }
        else if (columnJavaType == long.class)
        {
            filterType = new TypeToken<Filter<Long>>(){}.getType();
        }
        else if (columnJavaType == byte[].class)
        {
            filterType = new TypeToken<Filter<String>>(){}.getType();
        }
        else if (columnJavaType == Decimal.class)
        {
            filterType = new TypeToken<Filter<Long>>(){}.getType();
        }
        else
        {
            throw new IllegalArgumentException("column java type (" + columnJavaType.getName() +
                    ") is not supported in column filter");
        }
        this.filter = JSON.parseObject(filterJson, filterType);
        this.includes = new HashSet<>();
        this.excludes = new HashSet<>();
        for (Bound<T> discrete : this.filter.discreteValues)
        {
            if (discrete.type == Bound.Type.INCLUDED)
            {
                includes.add(discrete.value);
            }
            else
            {
                excludes.add(discrete.value);
            }
        }
    }

    /**
     * Get the estimation of the selectivity of this column filter.
     * @param nullFraction the fraction of the null values in this column
     * @param cardinality the cardinality of this column
     * @param columnStats the statistics of this column, mainly the min/max values
     * @return the estimated selectivity, for example 0.05 means 5%, negative if the selectivity can not be estimated
     */
    public double getSelectivity(double nullFraction, long cardinality, PixelsProto.ColumnStatistic columnStats)
    {
        checkArgument(nullFraction >= 0 && nullFraction <= 1, "nullFraction is not in the range [0, 1]");
        checkArgument(cardinality >= 0, "cardinality is negative");
        requireNonNull(columnStats, "stats is null");
        if (this.filter.isAll)
        {
            return 1.0;
        }
        if (this.filter.isNone)
        {
            return 0.0;
        }
        double selectivity = 0.0;
        if (this.filter.allowNull)
        {
            selectivity += nullFraction;
        }
        StatsRecorder stats = StatsRecorder.create(columnType, columnStats);
        if (this.filter.getRangeCount() > 0 && stats instanceof RangeStats<?>)
        {
            RangeStats<?> rangeStats = (RangeStats<?>) stats;
            switch (columnType)
            {
                case DATE:
                case TIME:
                case TIMESTAMP:
                case LONG:
                case DECIMAL:
                case INT:
                case SHORT:
                case BYTE:
                    for (Range<T> range : this.filter.ranges)
                    {
                        T lower = range.lowerBound.type != Bound.Type.UNBOUNDED ? range.lowerBound.value : null;
                        T upper = range.upperBound.type != Bound.Type.UNBOUNDED ? range.upperBound.value : null;
                        double s = rangeStats.getSelectivity(
                                lower, range.lowerBound.type == Bound.Type.INCLUDED,
                                upper, range.upperBound.type == Bound.Type.INCLUDED);
                        if (s > 0)
                        {
                            selectivity += s;
                        }
                    }
                    break;
                case DOUBLE:
                case FLOAT:
                    for (Range<T> range : this.filter.ranges)
                    {
                        Double lower = range.lowerBound.type != Bound.Type.UNBOUNDED ?
                                Double.longBitsToDouble((Long) range.lowerBound.value) : null;
                        Double upper = range.upperBound.type != Bound.Type.UNBOUNDED ?
                                Double.longBitsToDouble((Long) range.upperBound.value) : null;
                        double s = rangeStats.getSelectivity(
                                lower, range.lowerBound.type == Bound.Type.INCLUDED,
                                upper, range.upperBound.type == Bound.Type.INCLUDED);
                        if (s >= 0)
                        {
                            selectivity += s;
                        }
                    }
                    break;
                default:
                    selectivity = -1;
                    break;
            }
        }

        if (selectivity < 0)
        {
            return selectivity;
        }

        if (this.filter.getDiscreteValueCount() > 0)
        {
            checkArgument(this.filter.getDiscreteValueCount() <= cardinality,
                    "the discrete value count is larger than the cardinality of this column");
            selectivity += (this.filter.getDiscreteValueCount() / (double) cardinality);
        }

        if (selectivity > 1)
        {
            selectivity = 1;
        }

        return selectivity;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public TypeDescription.Category getColumnType()
    {
        return columnType;
    }

    public String getFilterJson()
    {
        if (this.filterJson == null)
        {
            this.filterJson = JSON.toJSONString(this.filter);
        }
        return this.filterJson;
    }

    public Filter<T> getFilter()
    {
        return filter;
    }

    /**
     * Filter the values in the column vector and set the bits in result for
     * matched values.
     * <br/>
     * <b>Notice 1:</b> this method sets the matched bits,
     * whereas other bits are cleared. All the implementation of this method
     * must follow this regulation.
     * <br/>
     * <b>Notice 2:</b> the bitset must be as long (number of bits) as the column vector.
     *
     * @param columnVector the column vector.
     * @param start the start offset in the column vector.
     * @param length the length to filter in the column vector
     * @param result the filtered result, in which the ith bit is set if the ith
     *               value in the column vector matches the filter.
     */
    public void doFilter(ColumnVector columnVector, int start, int length, Bitmap result)
    {
        if (this.filter.isAll)
        {
            result.set(start, start+length);
            return;
        }
        // start filtering, set the bits of all matched rows.
        result.clear(start, start+length);

        if (this.filter.onlyNull)
        {
            if (columnVector.noNulls)
            {
                return;
            }
            for (int i = start; i < start+length; ++i)
            {
                if (columnVector.isNull[i])
                {
                    result.set(i);
                }
            }
            return;
        }

        if (this.filter.isNone)
        {
            return;
        }

        if (columnVector.isRepeating())
        {
            /*
             * For simplicity, we flatten the column vector instead of
             * dealing with the repeating column vector.
             */
            columnVector.flatten(false, null, columnVector.getLength());
        }
        switch (this.columnType)
        {
            case BOOLEAN:
            case BYTE:
                ByteColumnVector bcv = (ByteColumnVector) columnVector;
                doFilter(bcv.vector, bcv.noNulls ? null : bcv.isNull, start, length, result);
                return;
            case SHORT:
            case INT:
            case LONG:
                LongColumnVector lcv = (LongColumnVector) columnVector;
                doFilter(lcv.vector, lcv.noNulls ? null : lcv.isNull, start, length, result);
                return;
            case DECIMAL:
                if (columnVector instanceof LongDecimalColumnVector)
                {
                    // TODO: support column filter for long decimal.
                    throw new UnsupportedOperationException("long decimal is currently not supported in column filter");
                }
                DecimalColumnVector decv = (DecimalColumnVector) columnVector;
                // doFilter(decv.vector, decv.noNulls ? null : decv.isNull,
                //         decv.precision, decv.scale, start, length, result);
                /*
                 * The values in the Decimal filter are Long.
                 * For performance considerations, we reuse the doFilter method for Long columns.
                 */
                doFilter(decv.vector, decv.noNulls ? null : decv.isNull, start, length, result);
                return;
            case FLOAT:
            case DOUBLE:
                DoubleColumnVector dcv = (DoubleColumnVector) columnVector;
                doFilter(dcv.vector, dcv.noNulls ? null : dcv.isNull, start, length, result);
                return;
            case STRING:
            case VARCHAR:
            case CHAR:
            case VARBINARY:
            case BINARY:
                if (columnVector instanceof BinaryColumnVector)
                {
                    BinaryColumnVector bicv = (BinaryColumnVector) columnVector;
                    doFilter(bicv.vector, bicv.start, bicv.lens, bicv.noNulls ? null :
                            bicv.isNull, start, length, result);
                }
                else
                {
                    // Issue #369: support filtering on dictionary column vector.
                    DictionaryColumnVector dictcv = (DictionaryColumnVector) columnVector;
                    doFilter(dictcv.dictArray, dictcv.dictOffsets, dictcv.ids,
                            dictcv.noNulls ? null : dictcv.isNull, start, length, result);
                }
                return;
            case DATE:
                DateColumnVector dacv = (DateColumnVector) columnVector;
                doFilter(dacv.dates, dacv.noNulls ? null : dacv.isNull, start, length, result);
                return;
            case TIME:
                TimeColumnVector tcv = (TimeColumnVector) columnVector;
                doFilter(tcv.times, tcv.noNulls ? null : tcv.isNull, start, length, result);
                return;
            case TIMESTAMP:
                TimestampColumnVector tscv = (TimestampColumnVector) columnVector;
                doFilter(tscv.times, tscv.noNulls ? null : tscv.isNull, start, length, result);
                return;
            default:
                throw new UnsupportedOperationException("column type (" +
                        columnType.getPrimaryName() + ") is not supported in column filter");
        }
    }

    private void doFilter(byte[] vector, boolean[] isNull, int start, int length, Bitmap result)
    {
        boolean noNulls = isNull == null;
        if (!this.filter.ranges.isEmpty())
        {
            for (Range<T> range : this.filter.ranges)
            {
                byte lowerBound = range.lowerBound.type != Bound.Type.UNBOUNDED ?
                        (Byte) range.lowerBound.value : Byte.MIN_VALUE;
                if (range.lowerBound.type == Bound.Type.EXCLUDED)
                {
                    lowerBound++;
                }
                byte upperBound = range.upperBound.type != Bound.Type.UNBOUNDED ?
                        (Byte) range.upperBound.value : Byte.MAX_VALUE;
                if (range.upperBound.type == Bound.Type.EXCLUDED)
                {
                    upperBound--;
                }
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || vector[i] >= lowerBound && vector[i] <= upperBound)
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) && vector[i] >= lowerBound && vector[i] <= upperBound)
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
        else
        {
            if (!includes.isEmpty())
            {
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || includes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) && includes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
            }
            if (!excludes.isEmpty())
            {
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || !excludes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) && !excludes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
    }

    private void doFilter(long[] vector, boolean[] isNull, int start, int length, Bitmap result)
    {
        boolean noNulls = isNull == null;
        if (!this.filter.ranges.isEmpty())
        {
            for (Range<T> range : this.filter.ranges)
            {
                long lowerBound = range.lowerBound.type != Bound.Type.UNBOUNDED ?
                        (Long) range.lowerBound.value : Long.MIN_VALUE;
                if (range.lowerBound.type == Bound.Type.EXCLUDED)
                {
                    lowerBound++;
                }
                long upperBound = range.upperBound.type != Bound.Type.UNBOUNDED ?
                        (Long) range.upperBound.value : Long.MAX_VALUE;
                if (range.upperBound.type == Bound.Type.EXCLUDED)
                {
                    upperBound--;
                }
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || vector[i] >= lowerBound && vector[i] <= upperBound)
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) && vector[i] >= lowerBound && vector[i] <= upperBound)
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
        else
        {
            if (!includes.isEmpty())
            {
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || includes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) && includes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
            }
            if (!excludes.isEmpty())
            {
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || !excludes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) && !excludes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
    }

    /**
     * For Decimal, the values in the filter are Long, therefore we create Decimals using the
     * same precision and scale in the column vector. However, this method is currently not
     * used. Instead, we reuse the doFilter method for Long type columns, for Decimal columns.
     *
     * @param vector the values in the column vector
     * @param isNull isNull array
     * @param precision the precision of the column vector
     * @param scale the scale of the column vector
     * @param start the offset in the column vector to start comparison
     * @param length the length in the column vector to compare
     * @param result the result bitmap, all the matched bits corresponding to the [start, start+length)
     *               range are set to true
     */
    private void doFilter(long[] vector, boolean[] isNull, int precision, int scale,
                          int start, int length, Bitmap result)
    {
        boolean noNulls = isNull == null;
        if (!this.filter.ranges.isEmpty())
        {
            for (Range<T> range : this.filter.ranges)
            {
                Decimal lowerBound = range.lowerBound.type != Bound.Type.UNBOUNDED ?
                        new Decimal((Long) range.lowerBound.value, precision, scale) :
                        new Decimal(Long.MIN_VALUE, 18, 0);
                if (range.lowerBound.type == Bound.Type.EXCLUDED)
                {
                    lowerBound.value++;
                }
                Decimal upperBound = range.upperBound.type != Bound.Type.UNBOUNDED ?
                        new Decimal((Long) range.upperBound.value, precision, scale) :
                        new Decimal(Long.MAX_VALUE, 18, 0);
                if (range.upperBound.type == Bound.Type.EXCLUDED)
                {
                    upperBound.value--;
                }
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || lowerBound.compareTo(vector[i], precision, scale) <= 0 &&
                                upperBound.compareTo(vector[i], precision, scale) >= 0)
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) &&
                                lowerBound.compareTo(vector[i], precision, scale) <= 0 &&
                                upperBound.compareTo(vector[i], precision, scale) >= 0)
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
        else
        {
            if (!includes.isEmpty())
            {
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || includes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) && includes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
            }
            if (!excludes.isEmpty())
            {
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || !excludes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) && !excludes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
    }

    /**
     * Notice that we currently only support comparing two Strings that are encoded in ASCII
     * characters. The code of these characters is in the range of [0, 127], whether using UTF8 or
     * ISO-8859-1 charset encoding.
     * <br/>
     * Therefore, if b1 and b2 represent two Strings, each character in the String is encoded to
     * only one byte with a positive value. Comparing b1 and b2 per byte, what we do in this method,
     * is equivalent to comparing the original Strings.
     *
     * @param b1 the first byte array
     * @param start1 the start offset in b1
     * @param len1 the number of bytes to compare in b1
     * @param b2 the second byte array
     * @param start2 the start offset in b2
     * @param len2 the number of bytes to compare in b2
     * @return
     */
    private int byteArrayCmp(byte[] b1, int start1, int len1, byte[] b2, int start2, int len2)
    {
        int lim = len1 < len2 ? len1 : len2;
        int c; // We only support ASCII characters, would not overflow.
        for (int i = start1, j = start2; i < lim; ++i, ++j)
        {
            c = b1[i] - b2[j];
            if (c != 0)
            {
                return c;
            }
        }
        return len1 - len2;
    }

    private void doFilter(byte[][] vector, int[] starts, int[] lens, boolean[] isNull,
                          int start, int length, Bitmap result)
    {
        boolean noNulls = isNull == null;
        if (!this.filter.ranges.isEmpty())
        {
            for (Range<T> range : this.filter.ranges)
            {
                boolean lowerBounded = range.lowerBound.type != Bound.Type.UNBOUNDED;
                boolean lowerIncluded = range.lowerBound.type == Bound.Type.INCLUDED;
                byte[] lowerBound =  (lowerBounded ?
                        (String) range.lowerBound.value : "").getBytes(StandardCharsets.UTF_8);
                boolean upperBounded = range.upperBound.type != Bound.Type.UNBOUNDED;
                boolean upperIncluded = range.upperBound.type == Bound.Type.INCLUDED;
                byte[] upperBound = (range.upperBound.type != Bound.Type.UNBOUNDED ?
                        (String) range.upperBound.value : "").getBytes(StandardCharsets.UTF_8);
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i])
                        {
                            result.set(i);
                            continue;
                        }
                        int cmp1 = lowerBounded ?
                                byteArrayCmp(vector[i], starts[i], lens[i], lowerBound, 0, lowerBound.length) : 1;
                        int cmp2 = upperBounded ?
                                byteArrayCmp(vector[i], starts[i], lens[i], upperBound, 0, upperBound.length) : -1;
                        if ((lowerIncluded ? cmp1 >= 0 : cmp1 > 0) && (upperIncluded ? cmp2 <= 0 : cmp2 < 0))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (!noNulls && isNull[i])
                        {
                            continue;
                        }
                        int cmp1 = lowerBounded ?
                                byteArrayCmp(vector[i], starts[i], lens[i], lowerBound, 0, lowerBound.length) : 1;
                        int cmp2 = upperBounded ?
                                byteArrayCmp(vector[i], starts[i], lens[i], upperBound, 0, upperBound.length) : -1;
                        if ((lowerIncluded ? cmp1 >= 0 : cmp1 > 0) && (upperIncluded ? cmp2 <= 0 : cmp2 < 0))
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
        else
        {
            if (!includes.isEmpty())
            {
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || includes.contains(
                                new String(vector[i], starts[i], lens[i], StandardCharsets.UTF_8)))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) && includes.contains(
                                new String(vector[i], starts[i], lens[i], StandardCharsets.UTF_8)))
                        {
                            result.set(i);
                        }
                    }
                }
            }
            if (!excludes.isEmpty())
            {
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || !excludes.contains(
                                new String(vector[i], starts[i], lens[i], StandardCharsets.UTF_8)))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) && !excludes.contains(
                                new String(vector[i], starts[i], lens[i], StandardCharsets.UTF_8)))
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
    }

    /**
     * This is used for {@link DictionaryColumnVector}.
     * @param dictArray the backing array of the dictionary.
     * @param dictOffsets the start offset of each value in the dictArray.
     * @param ids the dictionary ids (i.e., indexes) of the elements in the column vector.
     * @param isNull the isNull array of the elements in the column vector.
     * @param start the start offset in the column vector to filter.
     * @param length the number of elements to filter.
     * @param result the filtering result.
     */
    private void doFilter(byte[] dictArray, int[] dictOffsets, int[] ids, boolean[] isNull,
                          int start, int length, Bitmap result)
    {
        boolean noNulls = isNull == null;
        int dictOffset, dictLength;
        if (!this.filter.ranges.isEmpty())
        {
            for (Range<T> range : this.filter.ranges)
            {
                boolean lowerBounded = range.lowerBound.type != Bound.Type.UNBOUNDED;
                boolean lowerIncluded = range.lowerBound.type == Bound.Type.INCLUDED;
                byte[] lowerBound =  (lowerBounded ?
                        (String) range.lowerBound.value : "").getBytes(StandardCharsets.UTF_8);
                boolean upperBounded = range.upperBound.type != Bound.Type.UNBOUNDED;
                boolean upperIncluded = range.upperBound.type == Bound.Type.INCLUDED;
                byte[] upperBound = (range.upperBound.type != Bound.Type.UNBOUNDED ?
                        (String) range.upperBound.value : "").getBytes(StandardCharsets.UTF_8);
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i])
                        {
                            result.set(i);
                            continue;
                        }
                        dictOffset = dictOffsets[ids[i]];
                        dictLength = dictOffsets[ids[i] + 1] - dictOffset;
                        int cmp1 = lowerBounded ?
                                byteArrayCmp(dictArray, dictOffset, dictLength, lowerBound, 0, lowerBound.length) : 1;
                        int cmp2 = upperBounded ?
                                byteArrayCmp(dictArray, dictOffset, dictLength, upperBound, 0, upperBound.length) : -1;
                        if ((lowerIncluded ? cmp1 >= 0 : cmp1 > 0) && (upperIncluded ? cmp2 <= 0 : cmp2 < 0))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (!noNulls && isNull[i])
                        {
                            continue;
                        }
                        dictOffset = dictOffsets[ids[i]];
                        dictLength = dictOffsets[ids[i] + 1] - dictOffset;
                        int cmp1 = lowerBounded ?
                                byteArrayCmp(dictArray, dictOffset, dictLength, lowerBound, 0, lowerBound.length) : 1;
                        int cmp2 = upperBounded ?
                                byteArrayCmp(dictArray, dictOffset, dictLength, upperBound, 0, upperBound.length) : -1;
                        if ((lowerIncluded ? cmp1 >= 0 : cmp1 > 0) && (upperIncluded ? cmp2 <= 0 : cmp2 < 0))
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
        else
        {
            if (!includes.isEmpty())
            {
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        dictOffset = dictOffsets[ids[i]];
                        dictLength = dictOffsets[ids[i] + 1] - dictOffset;
                        if (isNull[i] || includes.contains(
                                new String(dictArray, dictOffset, dictLength, StandardCharsets.UTF_8)))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        dictOffset = dictOffsets[ids[i]];
                        dictLength = dictOffsets[ids[i] + 1] - dictOffset;
                        if ((noNulls || !isNull[i]) && includes.contains(
                                new String(dictArray, dictOffset, dictLength, StandardCharsets.UTF_8)))
                        {
                            result.set(i);
                        }
                    }
                }
            }
            if (!excludes.isEmpty())
            {
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        dictOffset = dictOffsets[ids[i]];
                        dictLength = dictOffsets[ids[i] + 1] - dictOffset;
                        if (isNull[i] || !excludes.contains(
                                new String(dictArray, dictOffset, dictLength, StandardCharsets.UTF_8)))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        dictOffset = dictOffsets[ids[i]];
                        dictLength = dictOffsets[ids[i] + 1] - dictOffset;
                        if ((noNulls || !isNull[i]) && !excludes.contains(
                                new String(dictArray, dictOffset, dictLength, StandardCharsets.UTF_8)))
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
    }

    private void doFilter(int[] vector, boolean[] isNull, int start, int length, Bitmap result)
    {
        boolean noNulls = isNull == null;
        if (!this.filter.ranges.isEmpty())
        {
            for (Range<T> range : this.filter.ranges)
            {
                int lowerBound = range.lowerBound.type != Bound.Type.UNBOUNDED ?
                        (Integer) range.lowerBound.value : Integer.MIN_VALUE;
                if (range.lowerBound.type == Bound.Type.EXCLUDED)
                {
                    lowerBound++;
                }
                int upperBound = range.upperBound.type != Bound.Type.UNBOUNDED ?
                        (Integer) range.upperBound.value : Integer.MAX_VALUE;
                if (range.upperBound.type == Bound.Type.EXCLUDED)
                {
                    upperBound--;
                }
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || vector[i] >= lowerBound && vector[i] <= upperBound)
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) && vector[i] >= lowerBound && vector[i] <= upperBound)
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
        else
        {
            if (!includes.isEmpty())
            {
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || includes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) && includes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
            }
            if (!excludes.isEmpty())
            {
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i] || !excludes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if ((noNulls || !isNull[i]) && !excludes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
    }
}
