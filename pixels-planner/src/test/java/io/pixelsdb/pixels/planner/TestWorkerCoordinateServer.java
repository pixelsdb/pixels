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

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.WorkerCoordinateException;
import io.pixelsdb.pixels.planner.coordinate.CFWorkerInfo;
import io.pixelsdb.pixels.planner.coordinate.PlanCoordinatorFactory;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateServer;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateService;
import io.pixelsdb.pixels.planner.plan.physical.Operator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.pixelsdb.pixels.planner.TestPixelsPlanner.CreateChainPartitionedBroadcastJoinOperator;

/**
 * @author hank
 * @create 2023-10-12
 */
public class TestWorkerCoordinateServer
{
    private WorkerCoordinateServer workerCoordinatorServer;
    private WorkerCoordinateService workerCoordinatorService;

    private final ExecutorService threadPool = Executors.newFixedThreadPool(1);

    @Before
    public void startServer() throws IOException, MetadataException
    {
        workerCoordinatorServer = new WorkerCoordinateServer(8088);
        workerCoordinatorService = new WorkerCoordinateService("localhost", 8088);
        threadPool.submit(workerCoordinatorServer);
        Operator joinOperator = CreateChainPartitionedBroadcastJoinOperator();
        PlanCoordinatorFactory.Instance().createPlanCoordinator(1000, joinOperator);
    }

    @Test
    public void testRegisterWorker() throws WorkerCoordinateException
    {
        CFWorkerInfo workerInfo = new CFWorkerInfo(
                "localhost", 8080, 1000, 1, "op1", null);
        workerCoordinatorService.registerWorker(workerInfo);
    }

    @After
    public void shutdownServer() throws InterruptedException
    {
        this.workerCoordinatorService.shutdown();
        this.workerCoordinatorServer.shutdown();
    }
}
