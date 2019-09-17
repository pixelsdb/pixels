/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.daemon.metric;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.Server;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestMetricServer
{
    @Test
    public void test () throws InterruptedException
    {
        ConfigFactory.Instance().addProperty("metric.node.text.dir", "/home/hank/");
        Server server = new MetricsServer();
        Thread thread = new Thread(server);
        thread.start();
        TimeUnit.SECONDS.sleep(5);
        System.exit(0);
    }
}
