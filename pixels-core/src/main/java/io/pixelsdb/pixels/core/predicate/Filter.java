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

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;

import java.util.ArrayList;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The filter (a set of conditions) for a group of data items (e.g., a column).
 * For discrete-value conditions,
 * included represents '=' or 'in' in SQL predicate, whereas excluded represents 'not in' in SQL predicate.
 * <p/>
 * <b>Notice:</b> A filter can be either range filer or discrete-value filter.
 * Therefore, either (lowerBounds, upperBounds) or discreteValues may have elements, but not both.
 * <p/>
 * Created at: 07/04/2022
 * Author: hank
 */
@JSONType(includes = {"javaType", "lowerBounds", "upperBounds", "discreteValues"})
public class Filter<T extends Comparable<T>>
{
    /**
     * The java type of T.
     */
    @JSONField(name = "javaType", ordinal = 0)
    public final Class<?> javaType;
    @JSONField(name = "ranges", ordinal = 4)
    public final ArrayList<Range<T>> ranges;
    @JSONField(name = "discreteValues", ordinal = 5)
    public final ArrayList<Bound<T>> discreteValues;
    @JSONField(name = "isAll", ordinal = 1)
    public final boolean isAll;
    @JSONField(name = "isNone", ordinal = 2)
    public final boolean isNone;
    @JSONField(name = "allowNull", ordinal = 3)
    public final boolean allowNull;
    @JSONField(name = "onlyNull", ordinal = 4)
    public final boolean onlyNull;

    public Filter(Class<?> javaType, boolean isAll, boolean isNone, boolean allowNull, boolean onlyNull)
    {
        this.javaType = javaType;
        this.ranges = new ArrayList<>();
        this.discreteValues = new ArrayList<>();
        this.isAll = isAll;
        this.isNone = isNone;
        this.allowNull = allowNull;
        this.onlyNull = onlyNull;
    }

    /**
     * This constructor is mainly used by fastjson.
     * @param javaType
     * @param ranges
     * @param discreteValues
     * @param isAll
     * @param isNone
     * @param allowNull
     */
    @JSONCreator
    public Filter(Class<?> javaType, ArrayList<Range<T>> ranges,
                  ArrayList<Bound<T>> discreteValues,
                  boolean isAll, boolean isNone, boolean allowNull, boolean onlyNull)
    {
        this.javaType = javaType;
        this.ranges = ranges;
        this.discreteValues = discreteValues;
        this.isAll = isAll;
        this.isNone = isNone;
        this.allowNull = allowNull;
        this.onlyNull = onlyNull;
    }

    public Range<T> getRange(int i)
    {
        checkArgument(i >= 0 && i < this.ranges.size(), "index out of bound");
        return ranges.get(i);
    }

    public void addRange(Bound<T> lowerBound, Bound<T> upperBound)
    {
        this.ranges.add(new Range<>(lowerBound, upperBound));
    }

    public Bound<T> getDiscreteValue(int i)
    {
        checkArgument(i >= 0 && i < this.discreteValues.size(), "index out of bound");
        return discreteValues.get(i);
    }

    public void addDiscreteValue(Bound<T> singleValue)
    {
        this.discreteValues.add(singleValue);
    }

    public int getRangeCount()
    {
        return this.ranges.size();
    }

    public int getDiscreteValueCount()
    {
        return this.discreteValues.size();
    }

    public Class<?> getJavaType()
    {
        return javaType;
    }

    public boolean isAll()
    {
        return isAll;
    }

    public boolean isNone()
    {
        return isNone;
    }

    public boolean isAllowNull()
    {
        return allowNull;
    }

    public boolean isOnlyNull()
    {
        return onlyNull;
    }
}
