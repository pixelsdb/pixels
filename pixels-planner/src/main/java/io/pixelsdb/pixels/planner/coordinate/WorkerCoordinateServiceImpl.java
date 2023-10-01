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

import java.util.List;

import static io.pixelsdb.pixels.common.error.ErrorCode.SUCCESS;

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
    public void getDownStreamWorkers(TurboProto.GetDownStreamWorkersRequest request,
                                     StreamObserver<TurboProto.GetDownStreamWorkersResponse> responseObserver)
    {
        long workerId = request.getWorkerId();
        Worker<CFWorkerInfo> worker = CFWorkerManager.Instance().getCFWorker(workerId);
        CFWorkerInfo workerInfo = worker.getWorkerInfo();
        PlanCoordinator planCoordinator = PlanCoordinatorFactory.Instance().getPlanCoordinator(workerInfo.getTransId());
        StageDependency dependency = planCoordinator.getStageDependency(workerInfo.getStageId());
        if (dependency != null)
        {
            boolean isWide = dependency.isWide();
            StageCoordinator dependentStage = planCoordinator.getStageCoordinator(dependency.getDownStreamStageId());
            dependentStage.waitForAllWorkersReady();
            List<Worker<CFWorkerInfo>> workers = dependentStage.getWorkers();
            if (isWide)
            {
                // TODO: add workers to response
            }
            else
            {
                // TODO: add the corresponding worker to response
            }
        }
        super.getDownStreamWorkers(request, responseObserver);
    }

    @Override
    public void getTasks(TurboProto.GetTasksRequest request, StreamObserver<TurboProto.GetTasksResponse> responseObserver)
    {
        super.getTasks(request, responseObserver);
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
