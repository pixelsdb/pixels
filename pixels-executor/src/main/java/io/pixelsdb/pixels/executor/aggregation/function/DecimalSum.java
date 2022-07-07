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

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.Integer128;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DecimalColumnVector;

/**
 * @author hank
 * @date 07/07/2022
 */
public class DecimalSum extends SingleColumnFunction
{
    private final TypeDescription outputType;
    private Integer128 longValue;
    private long shortValue = 0;
    private boolean isLong = false;

    protected DecimalSum(TypeDescription outputType)
    {
        this.outputType = outputType;
        if (outputType.getPrecision() > 18)
        {
            this.isLong = true;
            this.longValue = new Integer128(0L, 0L);
        }
    }

    @Override
    public void input(int rowId, ColumnVector inputVector)
    {
        if (inputVector.noNulls || !inputVector.isNull[rowId])
        {
            DecimalColumnVector decimalColumnVector = (DecimalColumnVector) inputVector;
            if (isLong)
            {
                longValue.add(decimalColumnVector.vector[rowId*2], decimalColumnVector.vector[rowId*2+1]);
            }
            else
            {
                shortValue += decimalColumnVector.vector[rowId];
            }
        }
    }

    @Override
    public void output(ColumnVector outputVector)
    {
        if (isLong)
        {
            outputVector.add(longValue);
        }
        else
        {
            outputVector.add(shortValue);
        }
    }

    @Override
    public Function clone()
    {
        return new DecimalSum(this.outputType);
    }
}
