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

/**
 * The value bound on for the filter on a data range (e.g., a column).
 * If the type of the bound is 'UNBOUNDED', the value might be null
 * and should not be used.
 *
 * Created at: 07/04/2022
 * Author: hank
 */
@JSONType(includes = {"type", "value"})
public class Bound<T extends Comparable<T>>
{
    public enum Type
    {
        UNBOUNDED, // this is an unbounded condition, such as an infinitive large or small value.
        INCLUDED,  // the value of this bound is included in the condition.
        EXCLUDED   // the value of this bound is excluded in the condition.
    }

    @JSONField(name = "type", ordinal = 0)
    public final Type type;
    @JSONField(name = "value", ordinal = 1)
    public final T value;

    /**
     * Construct a bound that is used in {@link Filter}.
     *
     * @param type the type of this bound
     * @param value the value of this bound, it can be null if the type is {@code UNBOUNDED}
     */
    @JSONCreator
    public Bound(Type type, T value)
    {
        this.type = type;
        this.value = value;
    }

    public Type getType()
    {
        return type;
    }

    public T getValue()
    {
        return value;
    }
}
