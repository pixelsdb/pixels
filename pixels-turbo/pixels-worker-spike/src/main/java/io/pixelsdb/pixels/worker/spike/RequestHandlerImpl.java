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
package io.pixelsdb.pixels.worker.spike;

import io.pixelsdb.pixels.spike.handler.RequestHandler;
import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.spike.handler.SpikeWorker;

public class RequestHandlerImpl implements RequestHandler
{
    @Override
    public SpikeWorker.CallWorkerFunctionResp execute(SpikeWorker.CallWorkerFunctionReq request)
    {
        try
        {
            // 获取请求的有效负载
            request.getRequestId();
            String payload = request.getPayload();
            WorkerRequest workerRequest = JSON.parseObject(payload, WorkerRequest.class);
            switch (workerRequest.getWorkerType())
            {
                case AGGREGATION:
                {
                    WorkerService<AggregationWorker, AggregationInput, AggregationOutput> service = new WorkerService<>(AggregationWorker.class, AggregationInput.class);
                    return service.execute(workerRequest.getWorkerPayload(), request.getRequestId());
                }
                case BROADCAST_CHAIN_JOIN:
                {
                    WorkerService<BroadcastChainJoinWorker, BroadcastChainJoinInput, JoinOutput> service = new WorkerService<>(BroadcastChainJoinWorker.class, BroadcastChainJoinInput.class);
                    return service.execute(workerRequest.getWorkerPayload(), request.getRequestId());
                }
                case BROADCAST_JOIN:
                {
                    WorkerService<BroadcastJoinWorker, BroadcastJoinInput, JoinOutput> service = new WorkerService<>(BroadcastJoinWorker.class, BroadcastJoinInput.class);
                    return service.execute(workerRequest.getWorkerPayload(), request.getRequestId());
                }
                case PARTITIONED_CHAIN_JOIN:
                {
                    WorkerService<PartitionedChainJoinWorker, PartitionedChainJoinInput, JoinOutput> service = new WorkerService<>(PartitionedChainJoinWorker.class, PartitionedChainJoinInput.class);
                    return service.execute(workerRequest.getWorkerPayload(), request.getRequestId());
                }
                case PARTITIONED_JOIN:
                {
                    WorkerService<PartitionedJoinWorker, PartitionedJoinInput, JoinOutput> service = new WorkerService<>(PartitionedJoinWorker.class, PartitionedJoinInput.class);
                    return service.execute(workerRequest.getWorkerPayload(), request.getRequestId());
                }
                case PARTITIONED_JOIN_STREAMING:
                {
                    WorkerService<PartitionedJoinStreamWorker, PartitionedJoinInput, JoinOutput> service = new WorkerService<>(PartitionedJoinStreamWorker.class, PartitionedJoinInput.class);
                    return service.execute(workerRequest.getWorkerPayload(), request.getRequestId());
                }
                case PARTITION:
                {
                    WorkerService<PartitionWorker, PartitionInput, PartitionOutput> service = new WorkerService<>(PartitionWorker.class, PartitionInput.class);
                    return service.execute(workerRequest.getWorkerPayload(), request.getRequestId());
                }
                case PARTITION_STREAMING:
                {
                    WorkerService<PartitionStreamWorker, PartitionInput, PartitionOutput> service = new WorkerService<>(PartitionStreamWorker.class, PartitionInput.class);
                    return service.execute(workerRequest.getWorkerPayload(), request.getRequestId());
                }
                case SCAN:
                {
                    WorkerService<ScanWorker, ScanInput, ScanOutput> service = new WorkerService<>(ScanWorker.class, ScanInput.class);
                    return service.execute(workerRequest.getWorkerPayload(), request.getRequestId());
                }
                case SCAN_STREAM:
                {
                    WorkerService<ScanStreamWorker, ScanInput, ScanOutput> service = new WorkerService<>(ScanStreamWorker.class, ScanInput.class);
                    return service.execute(workerRequest.getWorkerPayload(), request.getRequestId());
                }
                default:
                    throw new RuntimeException("Receive invalid worker type");
            }
        } catch (Exception e)
        {
            System.out.println("Error: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
