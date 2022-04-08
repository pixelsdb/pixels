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
import io.pixelsdb.pixels.core.vector.*;

import java.lang.reflect.Type;
import java.util.BitSet;

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
     * <b>Notice 1:</b> this method only clears the unmatched bits
     * whereas other bits are not modified. All the implementation of this method
     * must follow this regulation.
     * <br/>
     * <b>Notice 2:</b> the bitset must be as long (number of bits) as the column vector.
     *
     * @param columnVector the column vector.
     * @param start the start offset in the column vector.
     * @param length the length to filter in the column vector
     * @param result the filtered result, in which the ith bit is cleared if the ith
     *               value in the column vector does not match the filter.
     */
    public void doFilter(ColumnVector columnVector, int start, int length, BitSet result)
    {
        if (this.filter.isAll)
        {
            return;
        }
        if (this.filter.isNone)
        {
            result.clear(start, start+length);
            return;
        }
        switch (this.columnType)
        {
            case BOOLEAN:
            case BYTE:
                doFilter((ByteColumnVector) columnVector, start, length, result);
                return;
            case SHORT:
            case INT:
            case LONG:
                doFilter((LongColumnVector) columnVector, start, length, result);
                return;
            case DECIMAL:
                doFilter((DecimalColumnVector) columnVector, start, length, result);
                return;
            case FLOAT:
            case DOUBLE:
                doFilter((DoubleColumnVector) columnVector, start, length, result);
                return;
            case STRING:
            case VARCHAR:
            case CHAR:
            case VARBINARY:
            case BINARY:
                doFilter((BinaryColumnVector) columnVector, start, length, result);
                return;
            case DATE:
                doFilter((DateColumnVector) columnVector, start, length, result);
                return;
            case TIME:
                doFilter((TimeColumnVector) columnVector, start, length, result);
                return;
            case TIMESTAMP:
                doFilter((TimestampColumnVector) columnVector, start, length, result);
                return;
            default:
                throw new UnsupportedOperationException("column type (" +
                        columnType.getPrimaryName() + "is not supported in column filter");
        }
    }

    private void doFilter(ByteColumnVector columnVector, int start, int length, BitSet result)
    {
        if (!this.filter.ranges.isEmpty())
        {
            for (Range<T> range : this.filter.ranges)
            {
                boolean match = false;
                byte lowerBound = (Byte) range.lowerBound.value;
                byte upperBound = (Byte) range.lowerBound.value;
                if (this.filter.allowNull && !columnVector.noNulls)
                {
                    for (int i = start; i < start + length; ++i)
                    {
                        if (range.lowerBound.type == Bound.Type.INCLUDED)
                        {
                            //match = range.lowerBound.value.compareTo(columnVector.vector[i]);
                        }
                    }
                }
                else
                {
                    for (int i = start; i < start + length; ++i)
                    {
                    }
                }

            }
        }
        else
        {

        }
    }

    private void doFilter(LongColumnVector columnVector, int start, int length, BitSet result)
    {
        if (!this.filter.ranges.isEmpty())
        {

        }
        else
        {

        }
    }

    private void doFilter(DecimalColumnVector columnVector, int start, int length, BitSet result)
    {
        if (!this.filter.ranges.isEmpty())
        {

        }
        else
        {

        }
    }

    private void doFilter(DoubleColumnVector columnVector, int start, int length, BitSet result)
    {
        if (!this.filter.ranges.isEmpty())
        {

        }
        else
        {

        }
    }

    private void doFilter(BinaryColumnVector columnVector, int start, int length, BitSet result)
    {
        if (!this.filter.ranges.isEmpty())
        {

        }
        else
        {

        }
    }

    private void doFilter(DateColumnVector columnVector, int start, int length, BitSet result)
    {
        if (!this.filter.ranges.isEmpty())
        {

        }
        else
        {

        }
    }

    private void doFilter(TimeColumnVector columnVector, int start, int length, BitSet result)
    {
        if (!this.filter.ranges.isEmpty())
        {

        }
        else
        {

        }
    }

    private void doFilter(TimestampColumnVector columnVector, int start, int length, BitSet result)
    {
        if (!this.filter.ranges.isEmpty())
        {

        }
        else
        {

        }
    }
}
