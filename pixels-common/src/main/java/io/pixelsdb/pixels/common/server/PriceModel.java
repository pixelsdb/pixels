/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.common.server;

import io.pixelsdb.pixels.common.exception.QueryServerException;

/**
 * @author hank
 * @create 2024-04-10
 */
public class PriceModel
{
    private PriceModel () { }

    /**
     *
     * @param scanSize
     * @return
     */
    public static double billedCents(double scanSize, ExecutionHint executionHint)
    {
        switch (executionHint)
        {
            case IMMEDIATE:
                return scanSize / 1024 / 1024 / 2048;
            case RELAXED:
                return scanSize / 1024 / 1024 / 10240;
            case BEST_EFFORT:
                return scanSize / 1024 / 1024 / 20480;
            default:
                throw new QueryServerException("invalid execution hint for calculating billed cents");
        }
    }
}
