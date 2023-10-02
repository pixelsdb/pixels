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
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.exception.WorkerCoordinateException;
import io.pixelsdb.pixels.common.task.Lease;
import io.pixelsdb.pixels.common.task.Task;
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
        TurboProto.GetDownStreamWorkersResponse.Builder builder = TurboProto.GetDownStreamWorkersResponse.newBuilder();
        if (dependency != null)
        {
            boolean isWide = dependency.isWide();
            StageCoordinator dependentStage = planCoordinator.getStageCoordinator(dependency.getDownStreamStageId());
            dependentStage.waitForAllWorkersReady();
            List<Worker<CFWorkerInfo>> downStreamWorkers = dependentStage.getWorkers();
            if (isWide)
            {
                builder.setErrorCode(SUCCESS);
                for (Worker<CFWorkerInfo> downStreamWorker : downStreamWorkers)
                {
                    builder.addDownStreamWorkers(downStreamWorker.getWorkerInfo().toProto());
                }
            }
            else
            {
                int workerIndex = planCoordinator.getStageCoordinator(workerInfo.getStageId()).getWorkerIndex(workerId);
                if (workerIndex < 0)
                {
                    builder.setErrorCode(ErrorCode.WORKER_COORDINATE_WORKER_NOT_FOUND);
                }
                else
                {
                    if (workerIndex >= downStreamWorkers.size())
                    {
                        builder.setErrorCode(ErrorCode.WORKER_COORDINATE_NO_DOWNSTREAM);
                    }
                    else
                    {
                        // get the worker with the same index in the downstream stage as the downstream worker
                        builder.setErrorCode(SUCCESS);
                        builder.addDownStreamWorkers(downStreamWorkers.get(workerIndex).getWorkerInfo().toProto());
                    }
                }
            }
        }
        else
        {
            builder.setErrorCode(ErrorCode.WORKER_COORDINATE_NO_DOWNSTREAM);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getTasksToExecute(TurboProto.GetTasksToExecuteRequest request,
                                  StreamObserver<TurboProto.GetTasksToExecuteResponse> responseObserver)
    {
        long workerId = request.getWorkerId();
        Worker<CFWorkerInfo> worker = CFWorkerManager.Instance().getCFWorker(workerId);
        CFWorkerInfo workerInfo = worker.getWorkerInfo();
        PlanCoordinator planCoordinator = PlanCoordinatorFactory.Instance().getPlanCoordinator(workerInfo.getTransId());
        StageCoordinator stageCoordinator = planCoordinator.getStageCoordinator(workerInfo.getStageId());
        TurboProto.GetTasksToExecuteResponse.Builder builder = TurboProto.GetTasksToExecuteResponse.newBuilder();
        try
        {
            List<Task> tasks = stageCoordinator.getTasksToRun(workerId);
            if (tasks.isEmpty())
            {
                builder.setErrorCode(ErrorCode.WORKER_COORDINATE_END_OF_TASKS);
            }
            else
            {
                builder.setErrorCode(SUCCESS);
                for (Task task : tasks)
                {
                    builder.addTaskInputs(TurboProto.TaskInput.newBuilder()
                            .setTaskId(task.getTaskId()).setPayload(task.getPayload()));
                }
            }
        } catch (WorkerCoordinateException e)
        {
            builder.setErrorCode(ErrorCode.WORKER_COORDINATE_WORKER_NOT_FOUND);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void completeTasks(TurboProto.CompleteTasksRequest request,
                              StreamObserver<TurboProto.CompleteTasksResponse> responseObserver)
    {
        long workerId = request.getWorkerId();
        List<TurboProto.TaskOutput> taskOutputs = request.getTaskOutputsList();
        Worker<CFWorkerInfo> worker = CFWorkerManager.Instance().getCFWorker(workerId);
        CFWorkerInfo workerInfo = worker.getWorkerInfo();
        PlanCoordinator planCoordinator = PlanCoordinatorFactory.Instance().getPlanCoordinator(workerInfo.getTransId());
        StageCoordinator stageCoordinator = planCoordinator.getStageCoordinator(workerInfo.getStageId());
        TurboProto.CompleteTasksResponse.Builder builder = TurboProto.CompleteTasksResponse.newBuilder();
        for (TurboProto.TaskOutput taskOutput : taskOutputs)
        {
            stageCoordinator.completeTask(taskOutput.getTaskId(), taskOutput.getOutput());
        }
        builder.setErrorCode(SUCCESS);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void extendLease(TurboProto.ExtendLeaseRequest request,
                            StreamObserver<TurboProto.ExtendLeaseResponse> responseObserver)
    {
        super.extendLease(request, responseObserver);
    }

    @Override
    public void terminateWorker(TurboProto.TerminateWorkerRequest request,
                                StreamObserver<TurboProto.TerminateWorkerResponse> responseObserver)
    {
        super.terminateWorker(request, responseObserver);
    }
}
