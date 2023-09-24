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
package io.pixelsdb.pixels.planner.coordinate;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.task.Lease;
import io.pixelsdb.pixels.common.task.Worker;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.turbo.TurboProto;
import io.pixelsdb.pixels.turbo.WorkerCoordinateServiceGrpc;

import static io.pixelsdb.pixels.common.error.ErrorCode.*;

/**
 * @author hank
 * @create 2023-07-31
 */
public class WorkerCoordinateServiceImpl extends WorkerCoordinateServiceGrpc.WorkerCoordinateServiceImplBase
{
    private static final long WorkerLeasePeriodMs;

    static
    {
        WorkerLeasePeriodMs = Long.parseLong(ConfigFactory.Instance().getProperty("executor.worker.lease.period.ms"));
    }

    @Override
    public void registerWorker(TurboProto.RegisterWorkerRequest request,
                               StreamObserver<TurboProto.RegisterWorkerResponse> responseObserver)
    {
        CFWorkerInfo workerInfo = new CFWorkerInfo(request.getWorkerInfo());
        Lease lease = new Lease(WorkerLeasePeriodMs, System.currentTimeMillis());
        long workerId = CFWorkerManager.Instance().createWorkerId();
        Worker<CFWorkerInfo> worker = new Worker<>(workerId, lease, workerInfo);
        CFWorkerManager.Instance().registerCFWorker(worker);
        PlanCoordinator planCoordinator = PlanCoordinatorFactory.Instance().getPlanCoordinator(workerInfo.getTransId());
        StageCoordinator stageCoordinator = planCoordinator.getStageCoordinator(workerInfo.getStageId());
        stageCoordinator.addWorker(worker);
        TurboProto.RegisterWorkerResponse response = TurboProto.RegisterWorkerResponse.newBuilder()
                .setErrorCode(SUCCESS).setWorkerId(workerId).setLeasePeriodMs(lease.getPeriodMs())
                .setLeaseStartTimeMs(lease.getStartTimeMs()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getDownStreamWorkers(TurboProto.GetDownStreamWorkersRequest request, StreamObserver<TurboProto.GetDownStreamWorkersResponse> responseObserver)
    {
        super.getDownStreamWorkers(request, responseObserver);
    }

    @Override
    public void executeTasks(TurboProto.ExecuteTasksRequest request, StreamObserver<TurboProto.ExecuteTasksResponse> responseObserver)
    {
        super.executeTasks(request, responseObserver);
    }

    @Override
    public void completeTasks(TurboProto.CompleteTasksRequest request, StreamObserver<TurboProto.CompleteTasksResponse> responseObserver)
    {
        super.completeTasks(request, responseObserver);
    }

    @Override
    public void extendLease(TurboProto.ExtendLeaseRequest request, StreamObserver<TurboProto.ExtendLeaseResponse> responseObserver)
    {
        super.extendLease(request, responseObserver);
    }

    @Override
    public void terminateWorker(TurboProto.TerminateWorkerRequest request, StreamObserver<TurboProto.TerminateWorkerResponse> responseObserver)
    {
        super.terminateWorker(request, responseObserver);
    }
}
