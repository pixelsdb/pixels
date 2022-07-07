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
import io.pixelsdb.pixels.core.vector.LongColumnVector;

/**
 * @author hank
 * @date 07/07/2022
 */
public class BigintSum extends SingleColumnFunction
{
    private long value = 0;

    protected BigintSum() { }

    @Override
    public void input(int rowId, ColumnVector inputVector)
    {
        LongColumnVector longColumnVector = (LongColumnVector) inputVector;
        if (longColumnVector.noNulls || !longColumnVector.isNull[rowId])
        {
            this.value += longColumnVector.vector[rowId];
        }
    }

    @Override
    public void output(ColumnVector outputVector)
    {
        outputVector.add(value);
    }

    @Override
    public Function clone()
    {
        return new BigintSum();
    }
}
