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
package io.pixelsdb.pixels.daemon.scaling.util;

import org.junit.Test;

public class TestScalingManager
{
    @Test
    public void testExpandSome()
    {
        ScalingManager scalingManager = new ScalingManager();
        scalingManager.expandSome(3);
    }

    @Test
    public void testReduceSome()
    {
        ScalingManager scalingManager = new ScalingManager();
        scalingManager.reduceSome(2);
    }

    @Test
    public void testReduceAll()
    {
        ScalingManager scalingManager = new ScalingManager();
        scalingManager.reduceAll();
    }

    @Test
    public void testMultiplyInstance()
    {
        ScalingManager scalingManager = new ScalingManager();
        scalingManager.multiplyInstance(2);
        scalingManager.multiplyInstance(0.75f);
    }
}
