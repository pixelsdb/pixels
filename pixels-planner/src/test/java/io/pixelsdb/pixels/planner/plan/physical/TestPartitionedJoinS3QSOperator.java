/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.planner.plan.physical;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.lease.Lease;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.task.Task;
import io.pixelsdb.pixels.common.task.Worker;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.coordinate.CFWorkerInfo;
import io.pixelsdb.pixels.planner.coordinate.CoordinatorEndpoint;
import io.pixelsdb.pixels.planner.coordinate.PlanCoordinator;
import io.pixelsdb.pixels.planner.coordinate.StageCoordinator;
import io.pixelsdb.pixels.planner.coordinate.StageRuntimeController;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleQueueInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Haoting Yan
 * @create 2026-06-25
 */
public class TestPartitionedJoinS3QSOperator
{
    @Test
    public void acceptsCompleteShuffleInfo()
    {
        PartitionedJoinS3QSOperator.checkS3QSShuffleInfo(validShuffleInfo(), "test shuffle");
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsQueueWithoutNameOrUrl()
    {
        ShuffleInfo shuffleInfo = validShuffleInfo();
        shuffleInfo.setQueues(Collections.singletonList(new ShuffleQueueInfo(0, null, null)));

        PartitionedJoinS3QSOperator.checkS3QSShuffleInfo(shuffleInfo, "test shuffle");
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsPartitionInputWithoutProducerTaskId()
    {
        PartitionInput partitionInput = new PartitionInput();
        OutputInfo outputInfo = new OutputInfo("s3qs://bucket/shuffle/0/part",
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null), true);
        outputInfo.setShuffleInfo(validShuffleInfo());
        partitionInput.setOutput(outputInfo);

        PartitionedJoinS3QSOperator operator = new PartitionedJoinS3QSOperator("s3qs-join",
                Collections.singletonList(partitionInput), Collections.emptyList(), Collections.emptyList(),
                JoinAlgorithm.PARTITIONED);

        operator.executePrev();
    }

    @Test(expected = IllegalStateException.class)
    public void executeRejectsS3QSPlanWithoutCoordinator()
    {
        PartitionedJoinS3QSOperator operator = new PartitionedJoinS3QSOperator("s3qs-join",
                Collections.singletonList(partitionInput(0)), Collections.singletonList(partitionInput(1)),
                Collections.<JoinInput>singletonList(joinInput(validShuffleInfo())), JoinAlgorithm.PARTITIONED);

        operator.execute();
    }

    @Test
    public void initPlanCoordinatorCreatesPartitionTasksWithS3QSMetadata() throws Exception
    {
        PartitionInput smallPartition = partitionInput(0);
        smallPartition.setTableInfo(scanTableInfo("small"));
        PartitionInput largePartition = partitionInput(1);
        largePartition.setTableInfo(scanTableInfo("large"));
        PartitionedJoinS3QSOperator operator = new PartitionedJoinS3QSOperator("s3qs-join",
                Collections.singletonList(smallPartition), Collections.singletonList(largePartition),
                Collections.<JoinInput>singletonList(joinInput(validShuffleInfo())), JoinAlgorithm.PARTITIONED);
        PlanCoordinator planCoordinator = new PlanCoordinator(100L);

        operator.initPlanCoordinator(planCoordinator, -1, false);
        StageCoordinator smallStage = planCoordinator.getStageCoordinator(operator.smallPartitionStageId);
        smallStage.setDesiredRuntimeWorkerCount(1);
        Worker<CFWorkerInfo> worker = new Worker<>(1L, new Lease(System.currentTimeMillis(), 60000L),
                0, new CFWorkerInfo("localhost", 8080, 100L, operator.smallPartitionStageId,
                Constants.PARTITION_OPERATOR_NAME, Collections.emptyList()));
        smallStage.addWorker(worker);

        List<Task> tasks = smallStage.getTasksToRun(worker.getWorkerId());
        assertEquals(1, tasks.size());
        PartitionInput taskInput = JSON.parseObject(tasks.get(0).getPayload(), PartitionInput.class);
        assertEquals(0, taskInput.getProducerTaskId());
        assertEquals("shuffle-1", taskInput.getOutput().getShuffleInfo().getShuffleId());
        assertEquals(Storage.Scheme.s3qs, taskInput.getOutput().getShuffleInfo().getStorageInfo().getScheme());
        assertEquals(1, taskInput.getTableInfo().getInputSplits().size());
    }

    @Test
    public void initPlanCoordinatorUsesOneCoordinatorTaskPerS3QSProducerInput() throws Exception
    {
        PartitionInput smallPartition = partitionInput(7);
        smallPartition.setTableInfo(scanTableInfo("small", 3));
        PartitionInput largePartition = partitionInput(8);
        largePartition.setTableInfo(scanTableInfo("large"));
        PartitionedJoinS3QSOperator operator = new PartitionedJoinS3QSOperator("s3qs-join",
                Collections.singletonList(smallPartition), Collections.singletonList(largePartition),
                Collections.<JoinInput>singletonList(joinInput(validShuffleInfo())), JoinAlgorithm.PARTITIONED);
        PlanCoordinator planCoordinator = new PlanCoordinator(100L);

        operator.initPlanCoordinator(planCoordinator, -1, false);
        StageCoordinator smallStage = planCoordinator.getStageCoordinator(operator.smallPartitionStageId);
        smallStage.setDesiredRuntimeWorkerCount(1);
        assertEquals(1, smallStage.getTotalTaskCount());
        assertEquals(1, smallStage.getPendingTaskCount());
        assertEquals(0, smallStage.getRunningTaskCount());

        Worker<CFWorkerInfo> worker = new Worker<>(1L, new Lease(System.currentTimeMillis(), 60000L),
                0, new CFWorkerInfo("localhost", 8080, 100L, operator.smallPartitionStageId,
                Constants.PARTITION_OPERATOR_NAME, Collections.emptyList()));
        smallStage.addWorker(worker);

        List<Task> tasks = smallStage.getTasksToRun(worker.getWorkerId());
        assertEquals(1, tasks.size());
        assertEquals(0, tasks.get(0).getTaskId());
        assertEquals(0, smallStage.getPendingTaskCount());
        assertEquals(1, smallStage.getRunningTaskCount());
        PartitionInput taskInput = JSON.parseObject(tasks.get(0).getPayload(), PartitionInput.class);
        assertEquals(tasks.get(0).getTaskId(), taskInput.getProducerTaskId());
        assertEquals(3, taskInput.getTableInfo().getInputSplits().size());

        smallStage.completeTask(tasks.get(0).getTaskId(), true);
        assertEquals(0, smallStage.getRunningTaskCount());
        assertEquals(1, smallStage.getCompletedTaskCount());
        assertEquals(0, smallStage.getFailedTaskCount());
    }

    @Test
    public void executeStartsS3QSRuntimeWorkersWhenPlanCoordinatorIsInitialized() throws Exception
    {
        RecordingInvoker partitionStageInvoker = new RecordingInvoker();
        RecordingInvoker joinStageInvoker = new RecordingInvoker();
        Map<WorkerType, Invoker> originalInvokers = replaceS3QSInvokers(partitionStageInvoker, joinStageInvoker);
        try
        {
            ShuffleInfo smallShuffle = validShuffleInfo("small-shuffle");
            ShuffleInfo largeShuffle = validShuffleInfo("large-shuffle");
            PartitionInput smallPartition = partitionInput(7, smallShuffle);
            smallPartition.setTableInfo(scanTableInfo("small", 2));
            PartitionInput largePartition = partitionInput(8, largeShuffle);
            largePartition.setTableInfo(scanTableInfo("large", 2));
            PartitionedJoinInput joinInput = joinInput(smallShuffle, largeShuffle);
            PartitionedJoinS3QSOperator operator = new PartitionedJoinS3QSOperator("s3qs-join",
                    Collections.singletonList(smallPartition), Collections.singletonList(largePartition),
                    Collections.<JoinInput>singletonList(joinInput), JoinAlgorithm.PARTITIONED);
            PlanCoordinator planCoordinator =
                    new PlanCoordinator(100L, new CoordinatorEndpoint("coordinator.internal", 19000));
            operator.initPlanCoordinator(planCoordinator, -1, false);

            CompletableFuture<CompletableFuture<? extends Output>[]> future = operator.execute();
            CompletableFuture<? extends Output>[] joinOutputs = future.get();

            assertEquals(2, partitionStageInvoker.inputs.size());
            StageWorkerInput smallStageWorkerInput = (StageWorkerInput) partitionStageInvoker.inputs.get(0);
            StageWorkerInput largeStageWorkerInput = (StageWorkerInput) partitionStageInvoker.inputs.get(1);
            assertEquals(WorkerType.PARTITION_S3QS, smallStageWorkerInput.getWorkerType());
            assertEquals(WorkerType.PARTITION_S3QS, largeStageWorkerInput.getWorkerType());
            assertEquals(100L, smallStageWorkerInput.getTransId());
            assertEquals(100L, largeStageWorkerInput.getTransId());
            assertEquals(operator.smallPartitionStageId, smallStageWorkerInput.getStageId());
            assertEquals(operator.largePartitionStageId, largeStageWorkerInput.getStageId());
            assertEquals("coordinator.internal", smallStageWorkerInput.getCoordinatorHost());
            assertEquals(19000, smallStageWorkerInput.getCoordinatorPort());
            assertEquals(1, joinStageInvoker.inputs.size());
            StageWorkerInput joinStageWorkerInput = (StageWorkerInput) joinStageInvoker.inputs.get(0);
            assertEquals(WorkerType.PARTITIONED_JOIN_S3QS, joinStageWorkerInput.getWorkerType());
            assertEquals(100L, joinStageWorkerInput.getTransId());
            assertEquals(operator.joinStageId, joinStageWorkerInput.getStageId());
            assertEquals("coordinator.internal", joinStageWorkerInput.getCoordinatorHost());
            assertEquals(19000, joinStageWorkerInput.getCoordinatorPort());
            assertEquals(1, joinOutputs.length);

            StageCoordinator smallStage = planCoordinator.getStageCoordinator(operator.smallPartitionStageId);
            StageCoordinator largeStage = planCoordinator.getStageCoordinator(operator.largePartitionStageId);
            StageCoordinator joinStage = planCoordinator.getStageCoordinator(operator.joinStageId);
            StageRuntimeController smallRuntime =
                    planCoordinator.getStageRuntimeController(operator.smallPartitionStageId);
            StageRuntimeController largeRuntime =
                    planCoordinator.getStageRuntimeController(operator.largePartitionStageId);
            StageRuntimeController joinRuntime =
                    planCoordinator.getStageRuntimeController(operator.joinStageId);
            assertNotNull(smallRuntime);
            assertNotNull(largeRuntime);
            assertNotNull(joinRuntime);
            assertEquals(1, smallRuntime.getStatus().getDesiredWorkerCount());
            assertEquals(1, largeRuntime.getStatus().getDesiredWorkerCount());
            assertEquals(1, joinRuntime.getStatus().getDesiredWorkerCount());
            assertEquals(1, smallStage.getTotalTaskCount());
            assertEquals(1, largeStage.getTotalTaskCount());
            assertEquals(1, joinStage.getTotalTaskCount());
            assertEquals(1, smallStage.getPendingTaskCount());
            assertEquals(1, largeStage.getPendingTaskCount());
            assertEquals(1, joinStage.getPendingTaskCount());
            assertEquals(2, planCoordinator.getShuffleInfos().size());
        }
        finally
        {
            restoreInvokers(originalInvokers);
        }
    }

    @Test
    public void outputCollectionWaitsForAllKnownAttemptsAfterOneFails() throws Exception
    {
        CompletableFuture<Output> failedAttempt = new CompletableFuture<>();
        CompletableFuture<Output> runningAttempt = new CompletableFuture<>();

        CompletableFuture<Void> collectionBarrier = CompletableFuture.runAsync(() ->
        {
            try
            {
                PartitionedJoinS3QSOperator.waitForAllAttempts(
                        new CompletableFuture[] {failedAttempt, runningAttempt});
            }
            catch (ExecutionException | InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        });

        failedAttempt.completeExceptionally(new IllegalStateException("worker failed"));
        assertFalse(collectionBarrier.isDone());

        Output output = new Output() { };
        output.setSuccessful(true);
        runningAttempt.complete(output);
        collectionBarrier.get();
        assertTrue(collectionBarrier.isDone());
    }

    private static ShuffleInfo validShuffleInfo()
    {
        return validShuffleInfo("shuffle-1");
    }

    private static ShuffleInfo validShuffleInfo(String shuffleId)
    {
        return new ShuffleInfo(shuffleId,
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                "s3qs://bucket/shuffle/" + shuffleId + "/",
                1, 1, 1, 1,
                Collections.singletonList(new ShuffleQueueInfo(0, shuffleId + "-p0", null)));
    }

    private static PartitionInput partitionInput(int producerTaskId)
    {
        return partitionInput(producerTaskId, validShuffleInfo());
    }

    private static PartitionInput partitionInput(int producerTaskId, ShuffleInfo shuffleInfo)
    {
        PartitionInput partitionInput = new PartitionInput();
        OutputInfo outputInfo = new OutputInfo("s3qs://bucket/shuffle/" + producerTaskId + "/part",
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null), true);
        outputInfo.setShuffleInfo(shuffleInfo);
        partitionInput.setOutput(outputInfo);
        partitionInput.setProducerTaskId(producerTaskId);
        return partitionInput;
    }

    private static ScanTableInfo scanTableInfo(String tableName)
    {
        return scanTableInfo(tableName, 1);
    }

    private static ScanTableInfo scanTableInfo(String tableName, int splitCount)
    {
        List<InputSplit> inputSplits = new ArrayList<>(splitCount);
        for (int i = 0; i < splitCount; ++i)
        {
            inputSplits.add(new InputSplit(Collections.singletonList(
                    new InputInfo("s3://bucket/" + tableName + "-" + i + ".pxl", 0, 1))));
        }
        return new ScanTableInfo(tableName, true, new String[] {"key"},
                new StorageInfo(Storage.Scheme.s3, null, null, null, null),
                inputSplits, null);
    }

    private static PartitionedJoinInput joinInput(ShuffleInfo shuffleInfo)
    {
        return joinInput(shuffleInfo, shuffleInfo);
    }

    private static PartitionedJoinInput joinInput(ShuffleInfo smallShuffleInfo, ShuffleInfo largeShuffleInfo)
    {
        PartitionedJoinInput joinInput = new PartitionedJoinInput();
        joinInput.setSmallTable(tableInfo(smallShuffleInfo));
        joinInput.setLargeTable(tableInfo(largeShuffleInfo));
        return joinInput;
    }

    private static PartitionedTableInfo tableInfo(ShuffleInfo shuffleInfo)
    {
        PartitionedTableInfo tableInfo = new PartitionedTableInfo();
        tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3qs, null, null, null, null));
        tableInfo.setShuffleInfo(shuffleInfo);
        return tableInfo;
    }

    private static Map<WorkerType, Invoker> replaceS3QSInvokers(Invoker partitionInvoker, Invoker joinInvoker)
            throws Exception
    {
        Map<WorkerType, Invoker> invokerMap = invokerMap();
        Map<WorkerType, Invoker> original = new EnumMap<>(WorkerType.class);
        original.put(WorkerType.PARTITION_S3QS, invokerMap.get(WorkerType.PARTITION_S3QS));
        original.put(WorkerType.PARTITIONED_JOIN_S3QS, invokerMap.get(WorkerType.PARTITIONED_JOIN_S3QS));
        invokerMap.put(WorkerType.PARTITION_S3QS, partitionInvoker);
        invokerMap.put(WorkerType.PARTITIONED_JOIN_S3QS, joinInvoker);
        return original;
    }

    private static void restoreInvokers(Map<WorkerType, Invoker> original) throws Exception
    {
        Map<WorkerType, Invoker> invokerMap = invokerMap();
        for (Map.Entry<WorkerType, Invoker> entry : original.entrySet())
        {
            if (entry.getValue() == null)
            {
                invokerMap.remove(entry.getKey());
            }
            else
            {
                invokerMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<WorkerType, Invoker> invokerMap() throws Exception
    {
        Field field = InvokerFactory.class.getDeclaredField("invokerMap");
        field.setAccessible(true);
        return (Map<WorkerType, Invoker>) field.get(InvokerFactory.Instance());
    }

    private static class RecordingInvoker implements Invoker
    {
        private final List<Input> inputs = new ArrayList<>();

        @Override
        public Output parseOutput(String outputJson)
        {
            return null;
        }

        @Override
        public CompletableFuture<Output> invoke(Input input)
        {
            inputs.add(input);
            return CompletableFuture.completedFuture(new Output() { });
        }

        @Override
        public String getFunctionName()
        {
            return "recording";
        }

        @Override
        public int getMemoryMB()
        {
            return 0;
        }
    }

}
