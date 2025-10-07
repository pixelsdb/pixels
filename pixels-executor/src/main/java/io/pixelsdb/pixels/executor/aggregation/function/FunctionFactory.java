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
import io.pixelsdb.pixels.executor.aggregation.FunctionType;

/**
 * @author hank
 * @date 07/07/2022
 */
public class FunctionFactory
{
    private static final class InstanceHolder
    {
        private static final FunctionFactory instance = new FunctionFactory();
    }

    public static FunctionFactory Instance()
    {
        return InstanceHolder.instance;
    }

    private FunctionFactory() { }

    public Function createFunction(FunctionType functionType,
                                   TypeDescription inputType, TypeDescription outputType)
    {
        switch (functionType)
        {
            case SUM:
                return createSum(inputType, outputType);
            case COUNT:
                return new Count();
            default:
                // TODO: support more function types.
                throw new UnsupportedOperationException(
                        "function type '" + functionType +"' is not supported");
        }
    }

    private Function createSum(TypeDescription inputType, TypeDescription outputType)
    {
        switch (inputType.getCategory())
        {
            case SHORT:
            case INT:
            case LONG:
                return new BigintSum();
            case DECIMAL:
                return new DecimalSum(inputType, outputType);
            default:
                // TODO: support more types.
                throw new UnsupportedOperationException(
                        "input type '" + inputType.getCategory() +"' is not supported in SUM");
        }
    }
}
