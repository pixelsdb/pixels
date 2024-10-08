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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BasicPolicy extends Policy
{
    private static final Logger log = LogManager.getLogger(BasicPolicy.class);
    private static final int UPPER_BOUND = 3;
    private static final int LOWER_BOUND = 2;

    @Override
    public void doAutoScaling()
    {
        try
        {
            int queryConcurrency = metricsQueue.take();
            if (queryConcurrency < LOWER_BOUND)
            {
                log.info("Debug: reduce one vm");
                scalingManager.reduceOne();
            } else if (queryConcurrency > UPPER_BOUND)
            {
                log.info("Debug: expand one vm");
                scalingManager.expandOne();
            }
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
