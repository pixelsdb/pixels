/*
 * Copyright 2021 PixelsDB.
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
package io.pixelsdb.pixels.common.physical;

import io.pixelsdb.pixels.common.physical.impl.scheduler.NoopScheduler;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

/**
 * Created at: 9/10/21
 * Author: hank
 */
public class SchedulerFactory
{
    private static SchedulerFactory instance;

    public static SchedulerFactory Instance()
    {
        if (instance == null)
        {
            instance = new SchedulerFactory();
        }
        return instance;
    }

    private Scheduler scheduler;

    private SchedulerFactory()
    {
        String name = ConfigFactory.Instance().getProperty("read.request.scheduler");
        switch (name)
        {
            // Add more schedulers here.
            case "noop":
                scheduler = new NoopScheduler();
                break;
            default:
                throw new UnsupportedOperationException("The read request scheduler '" +
                        name + "' is unsupported.");
        }
    }

    public Scheduler getScheduler()
    {
        return scheduler;
    }
}
