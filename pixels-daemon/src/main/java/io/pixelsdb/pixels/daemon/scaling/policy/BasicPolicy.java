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

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.scaling.policy.helper.FixedSizeQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BasicPolicy extends Policy
{
    private static final Logger log = LogManager.getLogger(BasicPolicy.class);
    Integer reportPeriod = Integer.parseInt(ConfigFactory.Instance().getProperty("query.concurrency.report.period.sec"));
    private double lastAverage = 0;
    private int count = 0;
    private final int scalingInQueueSize = 5 * 60 / reportPeriod;
    private final int scalingOutQueueSize = 5 * 60 / reportPeriod;
    private FixedSizeQueue scalingInQueue = new FixedSizeQueue(scalingInQueueSize);
    private FixedSizeQueue scalingOutQueue = new FixedSizeQueue(scalingOutQueueSize);

    @Override
    public void doAutoScaling()
    {
        try
        {
            int queryConcurrency = metricsQueue.take();
            scalingInQueue.add(queryConcurrency);
            scalingOutQueue.add(queryConcurrency);
            count++;
            if (count >= scalingOutQueueSize)
            {
                count = 0;
                lastAverage = scalingOutQueue.getAverage();
                scalingOutQueue.clear();
            }

            if (scalingOutQueue.getAverage() > 2 && lastAverage > 2)
            {
                log.info("Debug: expand 100% vm");
                scalingManager.multiplyInstance(2.0f);
            } else if (scalingInQueue.getAverage() >= 0.25 && scalingInQueue.getAverage() < 0.5)
            {
                log.info("Debug: reduce 50% vm");
                scalingManager.multiplyInstance(0.5f);
            } else if (scalingInQueue.getAverage() >= 0 && scalingInQueue.getAverage() < 0.25)
            {
                log.info("Debug: reduce 75% vm");
                scalingManager.multiplyInstance(0.25f);
            }
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
