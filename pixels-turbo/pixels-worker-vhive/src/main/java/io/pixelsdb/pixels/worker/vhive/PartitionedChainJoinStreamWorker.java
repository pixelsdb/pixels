/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.planner.coordinate.CFWorkerInfo;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateService;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedChainJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.*;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;

import java.util.Collections;
public class PartitionedChainJoinStreamWorker extends BasePartitionedChainJoinStreamWorker implements RequestHandler<PartitionedChainJoinInput, JoinOutput>
{
    public PartitionedChainJoinStreamWorker(WorkerContext context)
    {
        super(context);
    }

    @Override
    public JoinOutput handleRequest(PartitionedChainJoinInput input)
    {
        long startTime = System.currentTimeMillis();
        try
        {
            int stageId = input.getStageId();
            long transId = input.getTransId();
            String ip = WorkerCommon.getIpAddress();
            int port = WorkerCommon.getPort();
            String coordinatorIp = WorkerCommon.getCoordinatorIp();
            int coordinatorPort = WorkerCommon.getCoordinatorPort();
            CFWorkerInfo workerInfo = new CFWorkerInfo(ip, port, transId, stageId, Constants.PARTITION_JOIN_OPERATOR_NAME, Collections.emptyList());

            workerCoordinatorService = new WorkerCoordinateService(coordinatorIp, coordinatorPort);
            worker = workerCoordinatorService.registerWorker(workerInfo);
            downStreamWorkers = workerCoordinatorService.getDownstreamWorkers(worker.getWorkerId());
            JoinOutput output = process(input);
            workerCoordinatorService.terminateWorker(worker.getWorkerId());
            workerCoordinatorService.shutdown();
            return output;
        } catch (Throwable e) {
            JoinOutput output = new JoinOutput();
            logger.error("error during registering worker", e);
            output.setSuccessful(false);
            output.setErrorMessage(e.getMessage());
            output.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return output;
        }
    }

    @Override
    public String getRequestId()
    {
        return this.context.getRequestId();
    }

    @Override
    public WorkerType getWorkerType() { return WorkerType.PARTITIONED_CHAIN_JOIN_STREAMING; }
}
