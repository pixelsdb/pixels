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
package io.pixelsdb.pixels.daemon.scaling.policy;

import io.pixelsdb.pixels.daemon.scaling.MetricsQueue;
import io.pixelsdb.pixels.daemon.scaling.util.ScalingManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingDeque;

public abstract class Policy
{
    static BlockingDeque<Integer> metricsQueue;
    ScalingManager scalingManager;

    public Policy()
    {
        metricsQueue = MetricsQueue.queue;
        scalingManager = new ScalingManager();
    }

    public abstract void doAutoScaling();

}
