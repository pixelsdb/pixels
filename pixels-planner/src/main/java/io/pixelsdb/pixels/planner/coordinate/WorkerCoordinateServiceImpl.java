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
        PlanCoordinator planCoordinator = PlanCoordinatorFactory.Instance().getPlanCoordinator(workerInfo.getTransId());
        StageCoordinator stageCoordinator = planCoordinator == null ? null :
                planCoordinator.getStageCoordinator(workerInfo.getStageId());
        if (stageCoordinator == null)
        {
            responseObserver.onNext(TurboProto.RegisterWorkerResponse.newBuilder()
                    .setErrorCode(ErrorCode.WORKER_COORDINATE_WORKER_NOT_FOUND).build());
            responseObserver.onCompleted();
            return;
        }
        Lease lease = new Lease(System.currentTimeMillis(), WorkerLeasePeriodMs);
        long workerId = CFWorkerManager.Instance().createWorkerId();
        Worker<CFWorkerInfo> worker = new Worker<>(workerId, lease, 0, workerInfo);
        synchronized (planCoordinator.getQueryState())
        {
            int error = SUCCESS;
            if (planCoordinator.getQueryState().getStatus() != QueryExecutionState.Status.RUNNING)
            {
                error = ErrorCode.WORKER_COORDINATE_QUERY_ABORTED;
            }
            else if (planCoordinator.getStageRuntimeController(workerInfo.getStageId()) != null)
            {
                if (workerInfo.getCancellationProtocolVersion() != 1)
                {
                    error = ErrorCode.WORKER_COORDINATE_PROTOCOL_MISMATCH;
                }
                else if (!planCoordinator.getQueryState().registerInvocation(
                        workerInfo.getInvocationId(), workerInfo.getStageId(), workerId))
                {
                    error = ErrorCode.WORKER_COORDINATE_TASK_REJECTED;
                }
            }
            if (error != SUCCESS)
            {
                responseObserver.onNext(TurboProto.RegisterWorkerResponse.newBuilder().setErrorCode(error).build());
                responseObserver.onCompleted();
                return;
            }
            stageCoordinator.addWorker(worker);
            CFWorkerManager.Instance().registerCFWorker(worker);
        }
        log.debug("register worker, local address: {}, transId: {}, stageId: {}, workerId: {}",
                workerInfo.getIp(), workerInfo.getTransId(), workerInfo.getStageId(), workerId);
        if (stageCoordinator.isQueued())
        {
            log.info("s3qs_worker_registered transId={} stageId={} workerId={} address={}",
                    workerInfo.getTransId(), workerInfo.getStageId(), workerId, workerInfo.getIp());
        }
        TurboProto.RegisterWorkerResponse response = TurboProto.RegisterWorkerResponse.newBuilder()
                .setCancellationProtocolVersion(1)
                .setErrorCode(SUCCESS).setWorkerId(workerId).setWorkerPortIndex(worker.getWorkerPortIndex()).setLeasePeriodMs(lease.getPeriodMs())
                .setLeaseStartTimeMs(lease.getStartMs()).build();
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
        if (worker == null)
        {
            responseObserver.onNext(TurboProto.GetTasksToExecuteResponse.newBuilder()
                    .setErrorCode(ErrorCode.WORKER_COORDINATE_WORKER_NOT_FOUND).build());
            responseObserver.onCompleted();
            return;
        }
        CFWorkerInfo workerInfo = worker.getWorkerInfo();
        PlanCoordinator planCoordinator = PlanCoordinatorFactory.Instance().getPlanCoordinator(workerInfo.getTransId());
        if (planCoordinator == null)
        {
            responseObserver.onNext(TurboProto.GetTasksToExecuteResponse.newBuilder()
                    .setErrorCode(ErrorCode.WORKER_COORDINATE_WORKER_NOT_FOUND).build());
            responseObserver.onCompleted();
            return;
        }
        StageCoordinator stageCoordinator = planCoordinator.getStageCoordinator(workerInfo.getStageId());
        TurboProto.GetTasksToExecuteResponse.Builder builder = TurboProto.GetTasksToExecuteResponse.newBuilder();
        try
        {
            List<Task> tasks;
            synchronized (planCoordinator.getQueryState())
            {
                planCoordinator.getQueryState().requireRunning();
                if (!worker.isAlive()) { throw new WorkerCoordinateException("worker lease expired"); }
                tasks = stageCoordinator.getTasksToRun(workerId);
            }
            if (tasks.isEmpty())
            {
                builder.setErrorCode(ErrorCode.WORKER_COORDINATE_END_OF_TASKS);
            }
            else
            {
                builder.setErrorCode(SUCCESS);
                for (Task task : tasks)
                {
                    if (stageCoordinator.isQueued())
                    {
                        log.info("s3qs_task_claimed transId={} stageId={} workerId={} taskId={}",
                                workerInfo.getTransId(), workerInfo.getStageId(), workerId, task.getTaskId());
                    }
                    builder.addTaskInputs(TurboProto.TaskInput.newBuilder()
                            .setTaskId(task.getTaskId()).setPayload(task.getPayload()));
                }
            }
        }
        catch (java.util.concurrent.CancellationException e)
        {
            builder.setErrorCode(ErrorCode.WORKER_COORDINATE_QUERY_ABORTED);
        }
        catch (WorkerCoordinateException e)
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
        TurboProto.CompleteTasksResponse.Builder builder = TurboProto.CompleteTasksResponse.newBuilder();
        PlanCoordinator planCoordinator = planFor(worker);
        if (planCoordinator == null)
        {
            builder.setErrorCode(ErrorCode.WORKER_COORDINATE_WORKER_NOT_FOUND);
        }
        else
        {
            CFWorkerInfo workerInfo = worker.getWorkerInfo();
            StageCoordinator stageCoordinator = planCoordinator.getStageCoordinator(workerInfo.getStageId());
            boolean failedTask = false;
            synchronized (planCoordinator.getQueryState())
            {
                if (planCoordinator.getQueryState().getStatus() != QueryExecutionState.Status.RUNNING)
                {
                    builder.setErrorCode(ErrorCode.WORKER_COORDINATE_QUERY_ABORTED);
                }
                else
                {
                    builder.setErrorCode(SUCCESS);
                    for (TurboProto.TaskResult result : taskResults)
                    {
                        if (!stageCoordinator.completeTask(result.getTaskId(), result.getSuccess(), workerId))
                        {
                            builder.setErrorCode(ErrorCode.WORKER_COORDINATE_TASK_REJECTED);
                            break;
                        }
                        failedTask |= !result.getSuccess();
                        if (stageCoordinator.isQueued())
                        {
                            log.info("s3qs_task_completed transId={} stageId={} workerId={} taskId={} success={}",
                                    workerInfo.getTransId(), workerInfo.getStageId(), workerId,
                                    result.getTaskId(), result.getSuccess());
                        }
                    }
                }
            }
            if (failedTask && planCoordinator.getStageRuntimeController(workerInfo.getStageId()) != null)
            { planCoordinator.getQueryState().abort(new IllegalStateException(
                    "worker " + workerId + " reported a failed task")); }
        }
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
        PlanCoordinator plan = planFor(worker);
        if (plan == null)
        {
            builder.setErrorCode(ErrorCode.WORKER_COORDINATE_WORKER_NOT_FOUND);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
            return;
        }
        try
        {
            synchronized (plan.getQueryState())
            {
                if (plan.getQueryState().getStatus() != QueryExecutionState.Status.RUNNING)
                {
                    builder.setErrorCode(ErrorCode.WORKER_COORDINATE_QUERY_ABORTED)
                            .setQueryStatus(TurboProto.QueryStatus.QUERY_ABORTED);
                }
                else
                {
                    builder.setErrorCode(SUCCESS).setLeaseStartTimeMs(worker.extendLease());
                }
            }
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
        if (worker == null)
        {
            responseObserver.onNext(builder.setErrorCode(ErrorCode.WORKER_COORDINATE_WORKER_NOT_FOUND).build());
            responseObserver.onCompleted();
            return;
        }
        CFWorkerInfo workerInfo = worker.getWorkerInfo();
        PlanCoordinator planCoordinator = planFor(worker);
        if (planCoordinator != null && !request.getFailureMessage().isEmpty())
        {
            planCoordinator.getQueryState().abort(new IllegalStateException(request.getFailureMessage()));
        }
        if (request.getQuiescent() || workerInfo.getCancellationProtocolVersion() == 0)
        {
            worker.terminate();
            if (planCoordinator != null && workerInfo.getCancellationProtocolVersion() == 1)
            {
                planCoordinator.getQueryState().workerStopped(workerInfo.getInvocationId(), workerId);
            }
        }
        if (worker.isTerminated() && planCoordinator != null &&
                planCoordinator.getStageRuntimeController(workerInfo.getStageId()) != null)
        {
            log.info("s3qs_worker_terminated transId={} stageId={} workerId={}",
                    workerInfo.getTransId(), workerInfo.getStageId(), workerId);
        }
        builder.setErrorCode(SUCCESS);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private static PlanCoordinator planFor(Worker<CFWorkerInfo> worker)
    {
        return worker == null ? null : PlanCoordinatorFactory.Instance().getPlanCoordinator(
                worker.getWorkerInfo().getTransId());
    }
}
