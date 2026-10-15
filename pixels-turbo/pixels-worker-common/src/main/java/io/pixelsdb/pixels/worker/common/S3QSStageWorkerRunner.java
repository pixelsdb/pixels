/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public License
 * along with Pixels.  If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.worker.common;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.task.Worker;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.planner.coordinate.CFWorkerInfo;
import io.pixelsdb.pixels.planner.coordinate.TaskBatch;
import io.pixelsdb.pixels.planner.coordinate.TaskInfo;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateService;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedChainJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Runs S3QS stage workers through the coordinator task protocol.
 *
 * This class deliberately lives above the concrete Base*Worker implementations:
 * platform wrappers only adapt Lambda/vHive/Spike requests, this runner owns the
 * register -> pull task -> execute -> complete loop, and Base*Worker still owns
 * the physical S3QS read/write work for one task payload.
 */
public class S3QSStageWorkerRunner
{
    private final WorkerContext context;
    private final WorkerCoordinateService workerCoordinateService;

    public S3QSStageWorkerRunner(WorkerContext context)
    {
        this.context = requireNonNull(context, "context is null");
        this.workerCoordinateService = null;
    }

    S3QSStageWorkerRunner(WorkerContext context, WorkerCoordinateService workerCoordinateService)
    {
        this.context = requireNonNull(context, "context is null");
        this.workerCoordinateService = requireNonNull(workerCoordinateService, "workerCoordinateService is null");
    }

    public PartitionOutput runPartition(StageWorkerInput input)
    {
        return run(input, Constants.PARTITION_OPERATOR_NAME, WorkerType.PARTITION_S3QS,
                PartitionInput.class, taskInput -> new BasePartitionWorker(context).process(taskInput),
                new PartitionOutput());
    }

    public JoinOutput runPartitionedJoin(StageWorkerInput input)
    {
        return run(input, Constants.PARTITION_JOIN_OPERATOR_NAME, WorkerType.PARTITIONED_JOIN_S3QS,
                PartitionedJoinInput.class, taskInput -> new BasePartitionedJoinWorker(context).process(taskInput),
                new JoinOutput());
    }

    public JoinOutput runPartitionedChainJoin(StageWorkerInput input)
    {
        return run(input, Constants.PARTITION_JOIN_OPERATOR_NAME, WorkerType.PARTITIONED_CHAIN_JOIN_S3QS,
                PartitionedChainJoinInput.class,
                taskInput -> new BasePartitionedChainJoinWorker(context).process(taskInput), new JoinOutput());
    }

    private <I, O extends Output> O run(StageWorkerInput input, String operatorName, WorkerType expectedWorkerType,
                                        Class<I> taskInputClass, TaskExecutor<I, O> taskExecutor, O aggregateOutput)
    {
        requireNonNull(input, "input is null");
        if (input.getWorkerType() != expectedWorkerType)
        {
            throw new WorkerException("unexpected S3QS stage worker type: " + input.getWorkerType());
        }

        long startTimeMs = System.currentTimeMillis();
        aggregateOutput.setStartTimeMs(startTimeMs);
        aggregateOutput.setRequestId(context.getRequestId());
        aggregateOutput.setSuccessful(true);
        aggregateOutput.setErrorMessage("");

        WorkerCoordinateService coordinateService = getWorkerCoordinateService(input);
        Worker<CFWorkerInfo> runtimeWorker = null;
        try
        {
            CFWorkerInfo workerInfo = new CFWorkerInfo(InetAddress.getLocalHost().getHostAddress(), -1,
                    input.getTransId(), input.getStageId(), operatorName, new ArrayList<>());
            runtimeWorker = coordinateService.registerWorker(workerInfo);

            TaskBatch taskBatch = coordinateService.getTasksToExecute(runtimeWorker.getWorkerId());
            while (!taskBatch.isEndOfTasks())
            {
                List<TaskInfo> taskInfos = taskBatch.getTasks();
                for (TaskInfo taskInfo : taskInfos)
                {
                    executeTask(taskInfo, taskInputClass, taskExecutor, aggregateOutput);
                }
                coordinateService.completeTasks(runtimeWorker.getWorkerId(), taskInfos);
                taskBatch = coordinateService.getTasksToExecute(runtimeWorker.getWorkerId());
            }
        }
        catch (Throwable e)
        {
            aggregateOutput.setSuccessful(false);
            aggregateOutput.setErrorMessage(e.getMessage());
            throw new WorkerException("failed to run S3QS stage worker", e);
        }
        finally
        {
            if (runtimeWorker != null)
            {
                try
                {
                    coordinateService.terminateWorker(runtimeWorker.getWorkerId());
                }
                catch (Throwable ignored)
                {
                    // The worker is already done from the caller's perspective.
                }
            }
            aggregateOutput.setDurationMs((int) (System.currentTimeMillis() - startTimeMs));
        }
        return aggregateOutput;
    }

    private WorkerCoordinateService getWorkerCoordinateService(StageWorkerInput input)
    {
        if (workerCoordinateService != null)
        {
            return workerCoordinateService;
        }
        return new WorkerCoordinateService(input.getCoordinatorHost(), input.getCoordinatorPort());
    }

    private <I, O extends Output> void executeTask(TaskInfo taskInfo, Class<I> taskInputClass,
                                                  TaskExecutor<I, O> taskExecutor, O aggregateOutput)
    {
        try
        {
            I taskInput = JSON.parseObject(taskInfo.getPayload(), taskInputClass);
            O taskOutput = taskExecutor.execute(taskInput);
            taskInfo.setSuccess(taskOutput.isSuccessful());
            mergeOutput(aggregateOutput, taskOutput);
        }
        catch (Throwable e)
        {
            taskInfo.setSuccess(false);
            throw new WorkerException("failed to execute S3QS coordinator task " + taskInfo.getTaskId(), e);
        }
    }

    private void mergeOutput(Output aggregateOutput, Output taskOutput)
    {
        if (taskOutput == null)
        {
            return;
        }
        aggregateOutput.setSuccessful(aggregateOutput.isSuccessful() && taskOutput.isSuccessful());
        if (!taskOutput.isSuccessful())
        {
            aggregateOutput.setErrorMessage(taskOutput.getErrorMessage());
        }
        for (String output : taskOutput.getOutputs())
        {
            aggregateOutput.addOutput(output);
        }
        aggregateOutput.setNumReadRequests(aggregateOutput.getNumReadRequests() + taskOutput.getNumReadRequests());
        aggregateOutput.setNumWriteRequests(aggregateOutput.getNumWriteRequests() + taskOutput.getNumWriteRequests());
        aggregateOutput.setTotalReadBytes(aggregateOutput.getTotalReadBytes() + taskOutput.getTotalReadBytes());
        aggregateOutput.setTotalWriteBytes(aggregateOutput.getTotalWriteBytes() + taskOutput.getTotalWriteBytes());
        aggregateOutput.setCumulativeInputCostMs(
                aggregateOutput.getCumulativeInputCostMs() + taskOutput.getCumulativeInputCostMs());
        aggregateOutput.setCumulativeComputeCostMs(
                aggregateOutput.getCumulativeComputeCostMs() + taskOutput.getCumulativeComputeCostMs());
        aggregateOutput.setCumulativeOutputCostMs(
                aggregateOutput.getCumulativeOutputCostMs() + taskOutput.getCumulativeOutputCostMs());

        if (aggregateOutput instanceof PartitionOutput && taskOutput instanceof PartitionOutput)
        {
            PartitionOutput aggregatePartitionOutput = (PartitionOutput) aggregateOutput;
            PartitionOutput taskPartitionOutput = (PartitionOutput) taskOutput;
            Set<Integer> hashValues = aggregatePartitionOutput.getHashValues();
            if (hashValues == null)
            {
                hashValues = new HashSet<>();
                aggregatePartitionOutput.setHashValues(hashValues);
            }
            if (taskPartitionOutput.getHashValues() != null)
            {
                hashValues.addAll(taskPartitionOutput.getHashValues());
            }
        }
    }

    private interface TaskExecutor<I, O extends Output>
    {
        O execute(I input);
    }
}
