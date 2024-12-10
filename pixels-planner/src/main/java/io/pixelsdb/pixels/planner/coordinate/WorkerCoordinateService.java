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

import com.google.common.collect.ImmutableList;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.exception.WorkerCoordinateException;
import io.pixelsdb.pixels.common.task.Lease;
import io.pixelsdb.pixels.common.task.Worker;
import io.pixelsdb.pixels.turbo.TurboProto;
import io.pixelsdb.pixels.turbo.WorkerCoordinateServiceGrpc;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.pixelsdb.pixels.common.error.ErrorCode.*;

/**
 * The coordinate service for the cloud function workers that are involved in pipelining query execution.
 *
 * @author hank
 * @create 2023-07-31
 */
public class WorkerCoordinateService
{
    private final ManagedChannel channel;
    private final WorkerCoordinateServiceGrpc.WorkerCoordinateServiceBlockingStub stub;

    public WorkerCoordinateService(String host, int port)
    {
        assert (host != null);
        assert (port > 0 && port <= 65535);
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = WorkerCoordinateServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException
    {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * When a cloud function worker is started, it must register itself in the coordinate service before doing
     * anything else.
     * @param workerInfo the basic information of this worker
     * @return the complete information of the registered worker, including the workerId
     * @throws WorkerCoordinateException
     */
    public Worker<CFWorkerInfo> registerWorker(CFWorkerInfo workerInfo) throws WorkerCoordinateException
    {
        TurboProto.RegisterWorkerRequest request = TurboProto.RegisterWorkerRequest.newBuilder()
                .setWorkerInfo(workerInfo.toProto()).build();
        TurboProto.RegisterWorkerResponse response = this.stub.registerWorker(request);
        if (response.getErrorCode() != SUCCESS)
        {
            throw new WorkerCoordinateException("failed to register worker, error code=" + response.getErrorCode());
        }
        return new Worker<>(response.getWorkerId(),
                new Lease(response.getLeasePeriodMs(), response.getLeaseStartTimeMs()), response.getWorkerPortIndex(), workerInfo);
    }

    /**
     * If this worker is not the end of the execution pipeline (i.e., belongs to the root of the query plan),
     * it must get the downstream worker or workers where it should send the intermediate results to.
     * @param workerId the id of this worker
     * @return the basic information of the downstream worker(s), or empty list if there is no downstream worker(s)
     * @throws WorkerCoordinateException if there are any other errors
     */
    public List<CFWorkerInfo> getDownstreamWorkers(long workerId) throws WorkerCoordinateException
    {
        TurboProto.GetDownstreamWorkersRequest request = TurboProto.GetDownstreamWorkersRequest.newBuilder()
                .setWorkerId(workerId).build();
        TurboProto.GetDownstreamWorkersResponse response = this.stub.getDownstreamWorkers(request);
        if (response.getErrorCode() != SUCCESS)
        {
            if (response.getErrorCode() == WORKER_COORDINATE_NO_DOWNSTREAM)
            {
                return Collections.emptyList();
            }
            throw new WorkerCoordinateException("failed to get downstream workers, error code=" + response.getErrorCode());
        }
        ImmutableList.Builder<CFWorkerInfo> builder = ImmutableList.builder();
        for (TurboProto.WorkerInfo workerInfo : response.getDownstreamWorkersList())
        {
            builder.add(new CFWorkerInfo(workerInfo));
        }
        return builder.build();
    }

    /**
     * For the operators without a fixed task parallelism, the worker must dynamically pull the tasks to execute after
     * the registration. The tasks are returned in batches. The worker should pull the tasks repeatedly until the endOfTasks
     * filed is true on the returned task batch.
     * @param workerId the id of this worker
     * @return the information of a batch of tasks to execute.
     * @throws WorkerCoordinateException
     */
    public TaskBatch getTasksToExecute(long workerId) throws WorkerCoordinateException
    {
        TurboProto.GetTasksToExecuteRequest request = TurboProto.GetTasksToExecuteRequest.newBuilder()
                .setWorkerId(workerId).build();
        TurboProto.GetTasksToExecuteResponse response = this.stub.getTasksToExecute(request);
        boolean endOfTasks = false;
        if (response.getErrorCode() != SUCCESS)
        {
            if (response.getErrorCode() == WORKER_COORDINATE_END_OF_TASKS)
            {
                endOfTasks = true;
            }
            else
            {
                throw new WorkerCoordinateException("failed to get tasks to execute, error code=" + response.getErrorCode());
            }
        }
        ImmutableList.Builder<TaskInfo> tasksBuilder = ImmutableList.builder();
        for (TurboProto.TaskInput taskInput : response.getTaskInputsList())
        {
            tasksBuilder.add(new TaskInfo(taskInput));
        }
        return new TaskBatch(endOfTasks, tasksBuilder.build());
    }

    /**
     * Tells the coordinate service that the tasks are completed.
     * @param workerId the id of this worker
     * @param tasks the tasks to complete
     * @throws WorkerCoordinateException
     */
    public void completeTasks(long workerId, List<TaskInfo> tasks) throws WorkerCoordinateException
    {

        TurboProto.CompleteTasksRequest.Builder request =
                TurboProto.CompleteTasksRequest.newBuilder().setWorkerId(workerId);
        for (TaskInfo taskInfo : tasks)
        {
            request.addTaskResults(taskInfo.toTaskResultProto());
        }
        TurboProto.CompleteTasksResponse response = this.stub.completeTasks(request.build());
        if (response.getErrorCode() != SUCCESS)
        {
            throw new WorkerCoordinateException("failed to complete tasks, error code=" + response.getErrorCode());
        }
    }

    /**
     * During the lifetime of a cloud function worker, it must extend its lease before the lease is expired.
     * @param workerId the id of this worker
     * @return the new start time in milliseconds since the Unix epoch of the extended lease
     * @throws WorkerCoordinateException if failed to extend the lease, in this case the worker must terminate itself
     * and stop working, the execution pipeline will be terminated and restarted by the coordinate service
     */
    public long extendLease(long workerId) throws WorkerCoordinateException
    {
        TurboProto.ExtendLeaseRequest request = TurboProto.ExtendLeaseRequest.newBuilder()
                .setWorkerId(workerId).build();
        TurboProto.ExtendLeaseResponse response = this.stub.extendLease(request);
        if (response.getErrorCode() != SUCCESS)
        {
            throw new WorkerCoordinateException("failed to extend lease, error code=" + response.getErrorCode());
        }
        return response.getLeaseStartTimeMs();
    }

    /**
     * Tell the coordinate service to terminate this worker.
     * @param workerId the id of this worker
     * @throws WorkerCoordinateException
     */
    public void terminateWorker(long workerId) throws WorkerCoordinateException
    {
        TurboProto.TerminateWorkerRequest request = TurboProto.TerminateWorkerRequest.newBuilder()
                .setWorkerId(workerId).build();
        TurboProto.TerminateWorkerResponse response = this.stub.terminateWorker(request);
        if (response.getErrorCode() != SUCCESS)
        {
            throw new WorkerCoordinateException("failed to terminate worker, error code=" + response.getErrorCode());
        }
    }
}
