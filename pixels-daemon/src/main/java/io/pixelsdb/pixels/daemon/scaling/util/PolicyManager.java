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

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.scaling.MetricsQueue;
import io.pixelsdb.pixels.daemon.scaling.policy.BasicPolicy;
import io.pixelsdb.pixels.daemon.scaling.policy.PidPolicy;
import io.pixelsdb.pixels.daemon.scaling.policy.Policy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PolicyManager implements Runnable
{
    private static final Logger log = LogManager
            .getLogger(PolicyManager.class);
    private final Policy policy;
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public PolicyManager()
    {
        ConfigFactory config = ConfigFactory.Instance();
        String policyName = config.getProperty("vm.auto.scaling.policy");
        switch (policyName)
        {
            case "pid":
                policy = new PidPolicy();
                break;
            default:
                policy = new BasicPolicy();
                break;
        }
    }

    @Override
    public void run()
    {
        scheduler.scheduleAtFixedRate(this::doAutoScaling, 0, 5, TimeUnit.SECONDS);
    }

    public void doAutoScaling()
    {
        policy.doAutoScaling();
    }
}
