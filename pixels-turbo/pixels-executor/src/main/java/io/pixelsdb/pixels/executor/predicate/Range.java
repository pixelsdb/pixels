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

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;

/**
 * A range of values.
 * It has a lower bound and an upper bound. Each bound can be of the type unbounded, included,
 * or excluded. The lower and upper bounds can be used to represent a valid interval in the
 * domain of target (e.g., a column).
 *
 * <p/>
 * Created at: 08/04/2022
 * Author: hank
 */
@JSONType(includes = {"lowerBound", "upperBound"})
public class Range<T extends Comparable<T>>
{
    @JSONField(name = "lowerBound", ordinal = 0)
    public final Bound<T> lowerBound;
    @JSONField(name = "upperBound", ordinal = 1)
    public final Bound<T> upperBound;

    @JSONCreator
    public Range(Bound<T> lowerBound, Bound<T> upperBound)
    {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }
}
