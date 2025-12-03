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
import io.pixelsdb.pixels.common.lease.Lease;
import io.pixelsdb.pixels.common.task.Task;
import io.pixelsdb.pixels.common.task.Worker;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.turbo.TurboProto;
import io.pixelsdb.pixels.turbo.WorkerCoordinateServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static io.pixelsdb.pixels.common.error.ErrorCode.SUCCESS;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2023-07-31
 */
public class WorkerCoordinateServiceImpl extends WorkerCoordinateServiceGrpc.WorkerCoordinateServiceImplBase
{
    private static final Logger log = LogManager.getLogger(WorkerCoordinateServiceImpl.class);
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
        Worker<CFWorkerInfo> worker = new Worker<>(workerId, lease, 0, workerInfo);
        CFWorkerManager.Instance().registerCFWorker(worker);
        log.debug("register worker, local address: {}, transId: {}, stageId: {}, workerId: {}",
                workerInfo.getIp(), workerInfo.getTransId(), workerInfo.getStageId(), workerId);
        PlanCoordinator planCoordinator = PlanCoordinatorFactory.Instance().getPlanCoordinator(workerInfo.getTransId());
        requireNonNull(planCoordinator, "plan coordinator is not found");
        StageCoordinator stageCoordinator = planCoordinator.getStageCoordinator(workerInfo.getStageId());
        requireNonNull(stageCoordinator, "stage coordinator is not found");
        stageCoordinator.addWorker(worker);
        TurboProto.RegisterWorkerResponse response = TurboProto.RegisterWorkerResponse.newBuilder()
                .setErrorCode(SUCCESS).setWorkerId(workerId).setWorkerPortIndex(worker.getWorkerPortIndex()).setLeasePeriodMs(lease.getPeriodMs())
                .setLeaseStartTimeMs(lease.getStartTimeMs()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getDownstreamWorkers(TurboProto.GetDownstreamWorkersRequest request,
                                     StreamObserver<TurboProto.GetDownstreamWorkersResponse> responseObserver)
    {
        long workerId = request.getWorkerId();
        Worker<CFWorkerInfo> worker = CFWorkerManager.Instance().getCFWorker(workerId);
        CFWorkerInfo workerInfo = worker.getWorkerInfo();
        PlanCoordinator planCoordinator = PlanCoordinatorFactory.Instance().getPlanCoordinator(workerInfo.getTransId());
        StageDependency dependency = planCoordinator.getStageDependency(workerInfo.getStageId());
        TurboProto.GetDownstreamWorkersResponse.Builder builder = TurboProto.GetDownstreamWorkersResponse.newBuilder();
        if (dependency.hasDownstreamStage())
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
                    builder.addDownstreamWorkers(downStreamWorker.getWorkerInfo().toProto());
                }
            }
            else
            {
                List<Integer> workerIndex = planCoordinator.getStageCoordinator(workerInfo.getStageId()).getWorkerIndex(workerId);
                if (workerIndex == null)
                {
                    builder.setErrorCode(ErrorCode.WORKER_COORDINATE_WORKER_NOT_FOUND);
                }
                else
                {
                    for (Integer index : workerIndex)
                    {
                        // get the worker with the same index in the downstream stage as the downstream worker
                        builder.setErrorCode(SUCCESS);
                        if (index < downStreamWorkers.size())
                        {
                            builder.addDownstreamWorkers(downStreamWorkers.get(index).getWorkerInfo().toProto());
                        }
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
        List<TurboProto.TaskResult> taskResults = request.getTaskResultsList();
        Worker<CFWorkerInfo> worker = CFWorkerManager.Instance().getCFWorker(workerId);
        CFWorkerInfo workerInfo = worker.getWorkerInfo();
        PlanCoordinator planCoordinator = PlanCoordinatorFactory.Instance().getPlanCoordinator(workerInfo.getTransId());
        StageCoordinator stageCoordinator = planCoordinator.getStageCoordinator(workerInfo.getStageId());
        TurboProto.CompleteTasksResponse.Builder builder = TurboProto.CompleteTasksResponse.newBuilder();
        for (TurboProto.TaskResult taskResult : taskResults)
        {
            stageCoordinator.completeTask(taskResult.getTaskId(), taskResult.getSuccess());
        }
        builder.setErrorCode(SUCCESS);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void extendLease(TurboProto.ExtendLeaseRequest request,
                            StreamObserver<TurboProto.ExtendLeaseResponse> responseObserver)
    {
        long workerId = request.getWorkerId();
        Worker<CFWorkerInfo> worker = CFWorkerManager.Instance().getCFWorker(workerId);
        TurboProto.ExtendLeaseResponse.Builder builder = TurboProto.ExtendLeaseResponse.newBuilder();
        try
        {
            long startTimeMs = worker.extendLease();
            builder.setErrorCode(SUCCESS);
            builder.setLeaseStartTimeMs(startTimeMs);
        } catch (WorkerCoordinateException e)
        {
            log.error("failed to extend the lease as the worker is not alive");
            builder.setErrorCode(ErrorCode.WORKER_COORDINATE_LEASE_EXPIRED);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void terminateWorker(TurboProto.TerminateWorkerRequest request,
                                StreamObserver<TurboProto.TerminateWorkerResponse> responseObserver)
    {
        long workerId = request.getWorkerId();
        Worker<CFWorkerInfo> worker = CFWorkerManager.Instance().getCFWorker(workerId);
        TurboProto.TerminateWorkerResponse.Builder builder = TurboProto.TerminateWorkerResponse.newBuilder();
        worker.terminate();
        builder.setErrorCode(SUCCESS);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}
