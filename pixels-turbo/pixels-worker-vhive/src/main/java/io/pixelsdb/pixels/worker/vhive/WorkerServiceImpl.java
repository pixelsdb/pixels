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
package io.pixelsdb.pixels.worker.vhive;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.turbo.TurboProto;
import io.pixelsdb.pixels.turbo.vHiveWorkerServiceGrpc;
import io.pixelsdb.pixels.worker.vhive.utils.ServiceImpl;

public class WorkerServiceImpl extends vHiveWorkerServiceGrpc.vHiveWorkerServiceImplBase
{
    public WorkerServiceImpl() { }

    @Override
    public void process(TurboProto.WorkerRequest request, StreamObserver<TurboProto.WorkerResponse> responseObserver)
    {
        WorkerType workerType = WorkerType.from(request.getWorkerType());
        switch (workerType)
        {
            case AGGREGATION:
            {
                ServiceImpl<AggregationWorker, AggregationInput, AggregationOutput> service = new ServiceImpl<>(AggregationWorker.class, AggregationInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case BROADCAST_CHAIN_JOIN:
            {
                ServiceImpl<BroadcastChainJoinWorker, BroadcastChainJoinInput, JoinOutput> service = new ServiceImpl<>(BroadcastChainJoinWorker.class, BroadcastChainJoinInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case BROADCAST_JOIN:
            {
                ServiceImpl<BroadcastJoinWorker, BroadcastJoinInput, JoinOutput> service = new ServiceImpl<>(BroadcastJoinWorker.class, BroadcastJoinInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case PARTITIONED_CHAIN_JOIN:
            {
                ServiceImpl<PartitionedChainJoinWorker, PartitionedChainJoinInput, JoinOutput> service = new ServiceImpl<>(PartitionedChainJoinWorker.class, PartitionedChainJoinInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case PARTITIONED_JOIN:
            {
                ServiceImpl<PartitionedJoinWorker, PartitionedJoinInput, JoinOutput> service = new ServiceImpl<>(PartitionedJoinWorker.class, PartitionedJoinInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case PARTITION:
            {
                ServiceImpl<PartitionWorker, PartitionInput, PartitionOutput> service = new ServiceImpl<>(PartitionWorker.class, PartitionInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case SCAN:
            {
                ServiceImpl<ScanWorker, ScanInput, ScanOutput> service = new ServiceImpl<>(ScanWorker.class, ScanInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case SCAN_STREAM:
            {
                ServiceImpl<ScanStreamWorker, ScanInput, ScanOutput> service = new ServiceImpl<>(ScanStreamWorker.class, ScanInput.class);
                service.execute(request, responseObserver);
                break;
            }
            default:
                throw new RuntimeException("Receive invalid function type");
        }
    }

    @Override
    public void getMemory(TurboProto.GetMemoryRequest request, StreamObserver<TurboProto.GetMemoryResponse> responseObserver)
    {
        // return the MB(1024 * 1024) size, represent the entire microVM memory size
        int dataSize = 1024 * 1024;
        TurboProto.GetMemoryResponse response = TurboProto.GetMemoryResponse.newBuilder()
                .setMemoryMB(Runtime.getRuntime().maxMemory() / dataSize)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
