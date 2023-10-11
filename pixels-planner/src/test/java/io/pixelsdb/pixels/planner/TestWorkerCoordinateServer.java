/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.planner;

import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author hank
 * @create 2023-10-12
 */
public class TestWorkerCoordinateServer
{
    private WorkerCoordinateServer server;
    private ExecutorService threadPool = Executors.newFixedThreadPool(1);

    @Before
    public void startServer()
    {
        server = new WorkerCoordinateServer(8088);
        threadPool.submit(server);
    }

    @Test
    public void test()
    {
        
    }

    @After
    public void shutdownServer()
    {
        this.server.shutdown();
    }
}
