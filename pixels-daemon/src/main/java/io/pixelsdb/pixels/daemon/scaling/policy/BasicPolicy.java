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

import io.pixelsdb.pixels.daemon.scaling.policy.helper.FixedSizeQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BasicPolicy extends Policy
{
    private static final Logger log = LogManager.getLogger(BasicPolicy.class);
    private static final double UPPER_BOUND = 3;
    private static final double LOWER_BOUND = 0.75;
    private FixedSizeQueue scalingInQueue = new FixedSizeQueue(60);
    private FixedSizeQueue scalingOutQueue = new FixedSizeQueue(30);

    @Override
    public void doAutoScaling()
    {
        try
        {
            int queryConcurrency = metricsQueue.take();
            scalingInQueue.add(queryConcurrency);
            scalingOutQueue.add(queryConcurrency);
            if (scalingInQueue.getAverage() <= LOWER_BOUND)
            {
                log.info("Debug: reduce 50% vm");
                scalingManager.multiplyInstance(0.5f);
            } else if (scalingOutQueue.getAverage() >= UPPER_BOUND)
            {
                log.info("Debug: expand 100% vm");
                scalingManager.multiplyInstance(2f);
            }
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
