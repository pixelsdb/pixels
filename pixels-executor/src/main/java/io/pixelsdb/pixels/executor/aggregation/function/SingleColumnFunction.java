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
 * @date 07/07/2022
 */
public abstract class SingleColumnFunction implements Function
{
    @Override
    public void input(int rowId, ColumnVector... inputVectors)
    {
        throw new UnsupportedOperationException("multi-column aggregation is not supported");
    }

    @Override
    public void output(ColumnVector... outputVectors)
    {
        throw new UnsupportedOperationException("multi-output aggregation is not supported");
    }
}
