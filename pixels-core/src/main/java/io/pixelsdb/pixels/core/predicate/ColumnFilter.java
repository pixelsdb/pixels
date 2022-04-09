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
package io.pixelsdb.pixels.core.predicate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;
import com.google.common.reflect.TypeToken;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.utils.Decimal;
import io.pixelsdb.pixels.core.vector.*;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

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

    public ColumnFilter(String columnName, TypeDescription.Category columnType, Filter<T> filter)
    {
        this.columnName = columnName;
        this.columnType = columnType;
        this.filter = filter;
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
            filterType = new TypeToken<Filter<Decimal>>(){}.getType();
        }
        else
        {
            throw new IllegalArgumentException("column java type (" + columnJavaType.getName() +
                    ") is not supported in column filter");
        }
        this.filter = JSON.parseObject(filterJson, filterType);
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
        if (this.filter.isNone)
        {
            return;
        }
        if (columnVector.isRepeating)
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
                DecimalColumnVector decv = (DecimalColumnVector) columnVector;
                doFilter(decv.vector, decv.noNulls ? null : decv.isNull,
                        decv.precision, decv.scale, start, length, result);
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
                BinaryColumnVector bicv = (BinaryColumnVector) columnVector;
                doFilter(bicv.vector, bicv.start, bicv.lens, bicv.noNulls ? null :
                        bicv.isNull, start, length, result);
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
                        columnType.getPrimaryName() + "is not supported in column filter");
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
                    lowerBound ++;
                }
                byte upperBound = range.upperBound.type != Bound.Type.UNBOUNDED ?
                        (Byte) range.upperBound.value : Byte.MAX_VALUE;
                if (range.lowerBound.type == Bound.Type.EXCLUDED)
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
                        if (vector[i] >= lowerBound && vector[i] <= upperBound)
                        {
                            result.set(i);
                        }
                    }
                }

            }
        }
        else
        {
            Set<Byte> includes = new HashSet<>();
            Set<Byte> excludes = new HashSet<>();
            for (Bound<T> discrete : this.filter.discreteValues)
            {
                if (discrete.type == Bound.Type.INCLUDED)
                {
                    includes.add((Byte) discrete.value);
                }
                else
                {
                    excludes.add((Byte) discrete.value);
                }
            }
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
                        if (includes.contains(vector[i]))
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
                        if (!excludes.contains(vector[i]))
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
                    lowerBound ++;
                }
                long upperBound = range.upperBound.type != Bound.Type.UNBOUNDED ?
                        (Long) range.upperBound.value : Long.MAX_VALUE;
                if (range.lowerBound.type == Bound.Type.EXCLUDED)
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
                        if (vector[i] >= lowerBound && vector[i] <= upperBound)
                        {
                            result.set(i);
                        }
                    }
                }

            }
        }
        else
        {
            Set<Long> includes = new HashSet<>();
            Set<Long> excludes = new HashSet<>();
            for (Bound<T> discrete : this.filter.discreteValues)
            {
                if (discrete.type == Bound.Type.INCLUDED)
                {
                    includes.add((Long) discrete.value);
                }
                else
                {
                    excludes.add((Long) discrete.value);
                }
            }
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
                        if (includes.contains(vector[i]))
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
                        if (!excludes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
    }

    private void doFilter(long[] vector, boolean[] isNull, int precision, int scale,
                          int start, int length, Bitmap result)
    {
        boolean noNulls = isNull == null;
        if (!this.filter.ranges.isEmpty())
        {
            for (Range<T> range : this.filter.ranges)
            {
                Decimal lowerBound = range.lowerBound.type != Bound.Type.UNBOUNDED ?
                        (Decimal) range.lowerBound.value : new Decimal(Long.MIN_VALUE, 18, 0);
                if (range.lowerBound.type == Bound.Type.EXCLUDED)
                {
                    lowerBound.value ++;
                }
                Decimal upperBound = range.upperBound.type != Bound.Type.UNBOUNDED ?
                        (Decimal) range.upperBound.value : new Decimal(Long.MAX_VALUE, 18, 0);
                if (range.lowerBound.type == Bound.Type.EXCLUDED)
                {
                    upperBound.value --;
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
                        if (lowerBound.compareTo(vector[i], precision, scale)<= 0 ||
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
            Set<Decimal> includes = new HashSet<>();
            Set<Decimal> excludes = new HashSet<>();
            for (Bound<T> discrete : this.filter.discreteValues)
            {
                if (discrete.type == Bound.Type.INCLUDED)
                {
                    includes.add((Decimal) discrete.value);
                }
                else
                {
                    excludes.add((Decimal) discrete.value);
                }
            }
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
                        if (includes.contains(vector[i]))
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
                        if (!excludes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
    }

    private int byteArrayCmp(byte[] b1, int start1, int len1, byte[] b2, int start2, int len2)
    {
        int lim = len1 < len2 ? len1 : len2;
        byte c1, c2;
        int k = start1, j = start2;
        while (k < lim) {
            c1 = b1[k];
            c2 = b2[j];
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
            j++;
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
            Set<String> includes = new HashSet<>();
            Set<String> excludes = new HashSet<>();
            for (Bound<T> discrete : this.filter.discreteValues)
            {
                if (discrete.type == Bound.Type.INCLUDED)
                {
                    includes.add((String) discrete.value);
                }
                else
                {
                    excludes.add((String) discrete.value);
                }
            }
            if (!includes.isEmpty())
            {
                if (this.filter.allowNull && !noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (isNull[i])
                        {
                            continue;
                        }
                        if (includes.contains(new String(vector[i], StandardCharsets.UTF_8)))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (includes.contains(new String(vector[i], StandardCharsets.UTF_8)))
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
                        if (isNull[i] || !excludes.contains(new String(vector[i], StandardCharsets.UTF_8)))
                        {
                            result.set(i);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (!excludes.contains(new String(vector[i], StandardCharsets.UTF_8)))
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
                    lowerBound ++;
                }
                int upperBound = range.upperBound.type != Bound.Type.UNBOUNDED ?
                        (Integer) range.upperBound.value : Integer.MAX_VALUE;
                if (range.lowerBound.type == Bound.Type.EXCLUDED)
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
                        if (vector[i] >= lowerBound && vector[i] <= upperBound)
                        {
                            result.set(i);
                        }
                    }
                }

            }
        }
        else
        {
            Set<Integer> includes = new HashSet<>();
            Set<Integer> excludes = new HashSet<>();
            for (Bound<T> discrete : this.filter.discreteValues)
            {
                if (discrete.type == Bound.Type.INCLUDED)
                {
                    includes.add((Integer) discrete.value);
                }
                else
                {
                    excludes.add((Integer) discrete.value);
                }
            }
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
                        if (includes.contains(vector[i]))
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
                        if (!excludes.contains(vector[i]))
                        {
                            result.set(i);
                        }
                    }
                }
            }
        }
    }
}
