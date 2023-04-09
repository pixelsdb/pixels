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
package io.pixelsdb.pixels.executor.aggregation.function;

import io.pixelsdb.pixels.core.vector.ColumnVector;

/**
 * @author hank
 * @date 9/1/22
 */
public class Count extends SingleColumnFunction
{
    private long value = 0;

    protected Count() { }

    @Override
    public void input(int rowId, ColumnVector inputVector)
    {
        if (inputVector.noNulls || !inputVector.isNull[rowId])
        {
            this.value++;
        }
    }

    @Override
    public void output(ColumnVector outputVector)
    {
        outputVector.add(value);
    }

    @Override
    public Function buildCopy()
    {
        return new Count();
    }
}
