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
package io.pixelsdb.pixels.daemon.scaling;

import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestScalingMetricsServer
{
    ScalingMetricsServer server;
    MockClient client;

    @Test
    public void testReportOneMetric()
    {
        server = new ScalingMetricsServer(54333);
        new Thread(() -> server.run()).start();
        // wait monitor server to start
        try
        {
            Thread.sleep(5000);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        client = new MockClient(54333);
        client.reportMetric(10);

        try
        {
            Thread.sleep(10000);
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testReportMetrics()
    {
        ScalingMetricsServer server = new ScalingMetricsServer(54333);
        new Thread(server).start();
        // wait monitor server to start
        try
        {
            Thread.sleep(5000);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        client = new MockClient(54333);

        ArrayList<Integer> metrics = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4, 5));
        Random random = new Random();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            client.reportMetric(metrics.get(random.nextInt(metrics.size())));
        }, 0, 3, TimeUnit.SECONDS);

        try
        {
            Thread.sleep(10000);
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    @After
    public void close()
    {
        if (client != null)
        {
            client.stopReportMetric();
        }
        if (server != null)
        {
            server.shutdown();
        }
    }
}
