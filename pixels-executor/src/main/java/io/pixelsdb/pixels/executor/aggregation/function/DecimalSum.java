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
import io.pixelsdb.pixels.core.vector.LongDecimalColumnVector;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.core.TypeDescription.MAX_SHORT_DECIMAL_SCALE;

/**
 * @author hank
 * @date 07/07/2022
 */
public class DecimalSum extends SingleColumnFunction
{
    private final TypeDescription inputType;
    private final TypeDescription outputType;
    private final boolean inputIsLong;
    private final boolean outputIsLong;
    private Integer128 longValue;
    private long shortValue = 0;

    protected DecimalSum(TypeDescription inputType, TypeDescription outputType)
    {
        this.inputType = inputType;
        this.outputType = outputType;
        this.inputIsLong = inputType.getPrecision() > MAX_SHORT_DECIMAL_SCALE;

        if (outputType.getPrecision() > MAX_SHORT_DECIMAL_SCALE)
        {
            this.outputIsLong = true;
            this.longValue = new Integer128(0L, 0L);
        }
        else
        {
            this.outputIsLong = false;
        }

        // Issue #647: check input and output data type.
        checkArgument(!this.inputIsLong || this.outputIsLong,
                "the output type must be long decimal if the input type is long decimal");
    }

    @Override
    public void input(int rowId, ColumnVector inputVector)
    {
        if (inputVector.noNulls || !inputVector.isNull[rowId])
        {
            if (inputIsLong)
            {
                LongDecimalColumnVector decimalColumnVector = (LongDecimalColumnVector) inputVector;
                // Issue #647: if the input type is long decimal, the output type must be long decimal too.
                longValue.add(decimalColumnVector.vector[rowId * 2], decimalColumnVector.vector[rowId * 2 + 1]);
            }
            else
            {
                DecimalColumnVector decimalColumnVector = (DecimalColumnVector) inputVector;
                // Issue #647: if the input type is short decimal, the output could be short or long decimal.
                if (outputIsLong)
                {
                    longValue.add(0L, decimalColumnVector.vector[rowId]);
                } else
                {
                    shortValue += decimalColumnVector.vector[rowId];
                }
            }
        }
    }

    @Override
    public void output(ColumnVector outputVector)
    {
        if (outputIsLong)
        {
            outputVector.add(longValue);
        }
        else
        {
            outputVector.add(shortValue);
        }
    }

    @Override
    public Function buildCopy()
    {
        return new DecimalSum(this.inputType, this.outputType);
    }
}
