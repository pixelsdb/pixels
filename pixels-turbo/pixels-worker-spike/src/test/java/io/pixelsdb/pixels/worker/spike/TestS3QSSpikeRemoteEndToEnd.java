/*
 * Copyright 2026 PixelsDB.
 *
 * Licensed under the GNU Affero General Public License, Version 3.
 */
package io.pixelsdb.pixels.worker.spike;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.common.task.Worker;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.coordinate.CoordinatedPlanExecution;
import io.pixelsdb.pixels.planner.coordinate.CoordinatorEndpoint;
import io.pixelsdb.pixels.planner.coordinate.CFWorkerInfo;
import io.pixelsdb.pixels.planner.coordinate.PlanCoordinator;
import io.pixelsdb.pixels.planner.coordinate.PlanCoordinatorFactory;
import io.pixelsdb.pixels.planner.coordinate.StageExecutionHandle;
import io.pixelsdb.pixels.planner.coordinate.StageCoordinator;
import io.pixelsdb.pixels.planner.coordinate.StageRuntimeStatus;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateServer;
import io.pixelsdb.pixels.planner.plan.physical.PartitionedJoinS3QSOperator;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleQueueInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import io.pixelsdb.pixels.worker.common.WorkerCommon;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Explicitly gated test using the production Spike invoker and a remote ECS worker.
 * Run baseline and scaling in separate JVMs with complete PIXELS_CONFIG files.
 */
public class TestS3QSSpikeRemoteEndToEnd
{
    @Test
    public void remoteSpikePartitionedJoin() throws Exception
    {
        assumeTrue("set PIXELS_S3QS_SPIKE_IT_ENABLED=true for remote verification",
                "true".equals(System.getenv("PIXELS_S3QS_SPIKE_IT_ENABLED")));
        String scenario = requiredEnv("PIXELS_S3QS_SPIKE_IT_SCENARIO");
        if (!"baseline".equals(scenario) && !"scaling".equals(scenario) &&
                !"isolation".equals(scenario) && !"draining".equals(scenario))
        {
            throw new IllegalArgumentException("unsupported remote scenario: " + scenario);
        }
        ConfigFactory config = ConfigFactory.Instance();
        assertEquals("spike", config.getProperty("executor.function.service"));
        assertTrue("producer function is missing", nonEmpty(config.getProperty("s3qs.partition.worker.name")));
        assertTrue("join function is missing", nonEmpty(config.getProperty("s3qs.partitioned.join.worker.name")));
        if ("isolation".equals(scenario))
        {
            Set<String> first = runScenario(true, false);
            Set<String> second = runScenario(true, false);
            first.retainAll(second);
            assertTrue("sequential queries reused an identity: " + first, first.isEmpty());
        }
        else
        {
            runScenario(!"baseline".equals(scenario), "draining".equals(scenario));
        }
    }

    private static Set<String> runScenario(boolean scaling, boolean manualDrain) throws Exception
    {
        String bucket = requiredEnv("PIXELS_S3QS_IT_BUCKET");
        String prefix = TestS3QSSpikeEndToEnd.trimSlashes(System.getenv("PIXELS_S3QS_IT_PREFIX"));
        String queuePrefix = requiredEnv("PIXELS_S3QS_IT_QUEUE_PREFIX");
        String advertisedHost = requiredEnv("PIXELS_S3QS_SPIKE_IT_COORDINATOR_HOST");
        int coordinatorPort = Integer.parseInt(requiredEnv("PIXELS_S3QS_SPIKE_IT_COORDINATOR_PORT"));
        int taskCount = scaling ? 12 : 1;
        int numPartitions = scaling ? 2 : 1;
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        long transId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
        long timestamp = System.currentTimeMillis();
        String root = bucket + "/" + (prefix.isEmpty() ? "" : prefix + "/") +
                "remote-" + (scaling ? "scaling" : "baseline") + "-" + suffix + "/";
        ShuffleInfo smallShuffle = scaling
                ? TestS3QSSpikeEndToEnd.shuffleInfo("small-" + suffix,
                        root + "shuffle/small/", queuePrefix + "-small-" + suffix, numPartitions)
                : TestS3QSSpikeEndToEnd.shuffleInfo("small-" + suffix,
                        root + "shuffle/small/", queuePrefix + "-small-" + suffix);
        ShuffleInfo largeShuffle = scaling
                ? TestS3QSSpikeEndToEnd.shuffleInfo("large-" + suffix,
                        root + "shuffle/large/", queuePrefix + "-large-" + suffix, numPartitions)
                : TestS3QSSpikeEndToEnd.shuffleInfo("large-" + suffix,
                        root + "shuffle/large/", queuePrefix + "-large-" + suffix);
        smallShuffle.setLogicalProducerTaskCount(taskCount);
        largeShuffle.setLogicalProducerTaskCount(taskCount);
        Set<String> runIdentities = new HashSet<>();
        runIdentities.add("transId:" + transId);
        runIdentities.add("shuffle:" + smallShuffle.getShuffleId());
        runIdentities.add("shuffle:" + largeShuffle.getShuffleId());
        smallShuffle.getQueues().forEach(queue -> runIdentities.add("queue:" + queue.getQueueName()));
        largeShuffle.getQueues().forEach(queue -> runIdentities.add("queue:" + queue.getQueueName()));

        Storage s3 = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
        List<PartitionInput> smallInputs = new ArrayList<>();
        List<PartitionInput> largeInputs = new ArrayList<>();
        Map<String, Integer> expected = new HashMap<>();
        for (int i = 0; i < taskCount; ++i)
        {
            String smallPath = root + "input/small-" + i + ".pxl";
            String largePath = root + "input/large-" + i + ".pxl";
            TestS3QSSpikeEndToEnd.writeInput(s3, smallPath, "small_value", scaling
                    ? new long[][] {{1, 10 + i}, {1, 10 + i}, {2, 20 + i}, {3, 30 + i}}
                    : new long[][] {{1, 10 + i}, {2, 20 + i}});
            TestS3QSSpikeEndToEnd.writeInput(s3, largePath, "large_value", scaling
                    ? new long[][] {{1, 100 + i}, {2, 200 + i}, {4, 400 + i}}
                    : new long[][] {{1, 100 + i}, {3, 300 + i}});
            PartitionInput small = TestS3QSSpikeEndToEnd.producerInput(transId, timestamp,
                    "small", smallPath, "small_value", smallShuffle);
            PartitionInput large = TestS3QSSpikeEndToEnd.producerInput(transId, timestamp,
                    "large", largePath, "large_value", largeShuffle);
            small.setProducerTaskId(i);
            large.setProducerTaskId(i);
            smallInputs.add(small);
            largeInputs.add(large);
        }
        for (int i = 0; i < taskCount; ++i)
        {
            for (int j = 0; j < taskCount; ++j)
            {
                expected.merge("1:" + (10 + i) + ":" + (100 + j), 1, Integer::sum);
                if (scaling)
                {
                    expected.merge("1:" + (10 + i) + ":" + (100 + j), 1, Integer::sum);
                    expected.merge("2:" + (20 + i) + ":" + (200 + j), 1, Integer::sum);
                }
            }
        }

        WorkerCoordinateServer server = new WorkerCoordinateServer(coordinatorPort);
        Thread serverThread = new Thread(server, "issue1277-remote-coordinator");
        serverThread.setDaemon(true);
        serverThread.start();
        long readyDeadline = System.currentTimeMillis() + 10000;
        while (!server.isRunning() && System.currentTimeMillis() < readyDeadline)
        {
            Thread.sleep(50);
        }
        assertTrue("Coordinator did not start on port " + coordinatorPort, server.isRunning());

        CoordinatedPlanExecution execution = null;
        AtomicBoolean observing = new AtomicBoolean(true);
        Thread observer = null;
        AtomicIntegerArray maxDesired = new AtomicIntegerArray(2);
        AtomicIntegerArray maxRegistered = new AtomicIntegerArray(2);
        boolean completed = false;
        try
        {
            List<JoinInput> joinInputs = new ArrayList<>();
            List<String> resultPaths = new ArrayList<>();
            for (int partitionId = 0; partitionId < numPartitions; ++partitionId)
            {
                String fileName = scaling ? "join-" + partitionId + ".pxl" : "join.pxl";
                joinInputs.add(TestS3QSSpikeEndToEnd.joinInput(transId, timestamp,
                        smallShuffle, largeShuffle, root + "result/", partitionId, fileName));
                resultPaths.add(root + "result/" + fileName);
            }
            PartitionedJoinS3QSOperator operator = new PartitionedJoinS3QSOperator(
                    "issue1277-remote-" + suffix, smallInputs, largeInputs,
                    joinInputs, JoinAlgorithm.PARTITIONED);
            execution = PlanCoordinatorFactory.Instance().createPlanExecution(
                    transId, operator, new CoordinatorEndpoint(advertisedHost, coordinatorPort));
            PlanCoordinator coordinator = execution.getPlanCoordinator();
            int smallStageId = smallInputs.get(0).getStageId();
            int largeStageId = largeInputs.get(0).getStageId();
            int joinStageId = joinInputs.get(0).getStageId();
            observer = new Thread(() -> observe(coordinator, smallStageId, largeStageId,
                    joinStageId, observing, maxDesired, maxRegistered), "issue1277-remote-observer");
            observer.setDaemon(true);
            observer.start();
            System.out.println("ISSUE1277_REMOTE_RUN transId=" + transId + " root=" + root +
                    " smallQueue=" + smallShuffle.getQueues().get(0).getQueueName() +
                    " largeQueue=" + largeShuffle.getQueues().get(0).getQueueName());
            execution.execute();
            long deadline = System.currentTimeMillis() + 180000;
            boolean allStagesDone = false;
            boolean drainTriggered = false;
            boolean completedAfterDrain = false;
            int completedAtDrain = -1;
            while (System.currentTimeMillis() < deadline)
            {
                StageRuntimeStatus smallStatus = coordinator.getStageExecutionHandle(smallStageId).getStatus();
                if (manualDrain && !drainTriggered && smallStatus.getDesiredWorkerCount() >= 2 &&
                        smallStatus.getActiveRegisteredWorkerCount() >= 2 &&
                        smallStatus.getRunningTaskCount() >= 2 && smallStatus.getPendingTaskCount() > 0)
                {
                    completedAtDrain = smallStatus.getCompletedTaskCount();
                    coordinator.scaleStage(smallStageId, 1);
                    StageCoordinator stage = coordinator.getStageCoordinator(smallStageId);
                    long drainedWorkerId = -1;
                    for (Worker<CFWorkerInfo> worker : stage.getWorkers())
                    {
                        if (stage.isWorkerDraining(worker.getWorkerId()))
                        {
                            assertTrue("draining terminated a running Worker", !worker.isTerminated());
                            drainedWorkerId = worker.getWorkerId();
                        }
                    }
                    assertTrue("no registered Worker was marked draining", drainedWorkerId >= 0);
                    System.out.println("ISSUE1277_REMOTE_MANUAL_DRAIN stageId=" + smallStageId +
                            " workerId=" + drainedWorkerId + " completedAtDrain=" + completedAtDrain);
                    drainTriggered = true;
                }
                if (drainTriggered && smallStatus.getCompletedTaskCount() > completedAtDrain)
                {
                    completedAfterDrain = true;
                }
                allStagesDone = true;
                for (int stageId : new int[] {smallStageId, largeStageId, joinStageId})
                {
                    StageExecutionHandle handle = coordinator.getStageExecutionHandle(stageId);
                    allStagesDone &= handle.getCompletionFuture().isDone();
                    for (CompletableFuture<?> attempt : handle.getAttemptFutures())
                    {
                        if (attempt.isCompletedExceptionally())
                        {
                            try
                            {
                                attempt.join();
                            }
                            catch (CompletionException e)
                            {
                                throw new AssertionError("remote Spike attempt failed in stage " + stageId, e.getCause());
                            }
                        }
                    }
                }
                if (allStagesDone)
                {
                    break;
                }
                Thread.sleep(500);
            }
            if (!allStagesDone)
            {
                fail("remote Spike execution did not finish within 180 seconds");
            }
            if (manualDrain)
            {
                assertTrue("remote manual drain was not triggered during active work", drainTriggered);
                assertTrue("no task completed after remote drain", completedAfterDrain);
            }
            execution.collectOutputs();
            observing.set(false);
            observer.join(2000);

            Map<String, Integer> actual = new HashMap<>();
            for (String resultPath : resultPaths)
            {
                readRows(s3, resultPath).forEach((row, count) -> actual.merge(row, count, Integer::sum));
            }
            assertEquals(expected, actual);
            for (int stageId : new int[] {smallStageId, largeStageId, joinStageId})
            {
                StageExecutionHandle handle = coordinator.getStageExecutionHandle(stageId);
                StageRuntimeStatus status = handle.getStatus();
                assertEquals(status.getTotalTaskCount(), status.getCompletedTaskCount());
                assertEquals(0, status.getFailedTaskCount());
                Set<String> requestIds = new HashSet<>();
                handle.getCompletionFuture().get().forEach(output ->
                {
                    assertTrue("remote Spike requestId is missing", nonEmpty(output.getRequestId()));
                    assertTrue("duplicate Spike requestId", requestIds.add(output.getRequestId()));
                    System.out.println("ISSUE1277_REMOTE_OUTPUT stageId=" + stageId +
                            " requestId=" + output.getRequestId());
                });
            }
            if (scaling)
            {
                assertTrue("no post-initial producer scale-out observed",
                        maxDesired.get(0) > 1 || maxDesired.get(1) > 1);
                assertTrue("no concurrent remote producer registration observed",
                        maxRegistered.get(0) > 1 || maxRegistered.get(1) > 1);
                assertTrue("no dynamically launched producer attempt completed",
                        coordinator.getStageExecutionHandle(smallStageId).getAttemptFutures().size() > 1 ||
                        coordinator.getStageExecutionHandle(largeStageId).getAttemptFutures().size() > 1);
            }
            TestS3QSSpikeEndToEnd.assertShuffleObjectsDeleted(s3, smallShuffle.getObjectPathPrefix());
            TestS3QSSpikeEndToEnd.assertShuffleObjectsDeleted(s3, largeShuffle.getObjectPathPrefix());
            S3QS s3qs = (S3QS) StorageFactory.Instance().getStorage(Storage.Scheme.s3qs);
            for (ShuffleQueueInfo queue : smallShuffle.getQueues())
            {
                TestS3QSSpikeEndToEnd.assertQueueDeleted(s3qs, queue.getQueueName());
            }
            for (ShuffleQueueInfo queue : largeShuffle.getQueues())
            {
                TestS3QSSpikeEndToEnd.assertQueueDeleted(s3qs, queue.getQueueName());
            }
            completed = true;
        }
        finally
        {
            observing.set(false);
            if (observer != null)
            {
                observer.join(2000);
            }
            if (execution != null)
            {
                execution.close();
            }
            server.shutdown();
            if (completed)
            {
                s3.delete(root, true);
            }
            else
            {
                System.err.println("ISSUE1277_REMOTE_RESIDUE root=" + root + " transId=" + transId);
            }
        }
        return runIdentities;
    }

    private static void observe(PlanCoordinator coordinator, int smallStageId, int largeStageId,
                                int joinStageId, AtomicBoolean observing,
                                AtomicIntegerArray maxDesired, AtomicIntegerArray maxRegistered)
    {
        int[] stageIds = {smallStageId, largeStageId, joinStageId};
        while (observing.get())
        {
            for (int i = 0; i < stageIds.length; ++i)
            {
                StageExecutionHandle handle = coordinator.getStageExecutionHandle(stageIds[i]);
                StageRuntimeStatus status = handle.getStatus();
                System.out.println("ISSUE1277_STAGE_SAMPLE stageId=" + stageIds[i] +
                        " desired=" + status.getDesiredWorkerCount() +
                        " registered=" + status.getActiveRegisteredWorkerCount() +
                        " accepting=" + status.getAcceptingWorkerCount() +
                        " draining=" + status.getDrainingWorkerCount() +
                        " starting=" + status.getStartingAttemptCount() +
                        " pending=" + status.getPendingTaskCount() +
                        " running=" + status.getRunningTaskCount() +
                        " completed=" + status.getCompletedTaskCount() +
                        " failed=" + status.getFailedTaskCount());
                if (i < 2)
                {
                    maxDesired.accumulateAndGet(i, status.getDesiredWorkerCount(), Math::max);
                    maxRegistered.accumulateAndGet(i, status.getActiveRegisteredWorkerCount(), Math::max);
                }
            }
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static Map<String, Integer> readRows(Storage storage, String path) throws IOException
    {
        PixelsReaderOption option = new PixelsReaderOption()
                .skipCorruptRecords(false)
                .tolerantSchemaEvolution(false)
                .includeCols(new String[] {"key", "small_value", "large_value"});
        Map<String, Integer> rows = new HashMap<>();
        try (PixelsReader reader = WorkerCommon.getReader(path, storage);
             PixelsRecordReader recordReader = reader.read(option))
        {
            VectorizedRowBatch batch;
            do
            {
                batch = recordReader.readBatch(32);
                LongColumnVector key = (LongColumnVector) batch.cols[0];
                LongColumnVector small = (LongColumnVector) batch.cols[1];
                LongColumnVector large = (LongColumnVector) batch.cols[2];
                for (int i = 0; i < batch.size; ++i)
                {
                    rows.merge(key.vector[i] + ":" + small.vector[i] + ":" + large.vector[i],
                            1, Integer::sum);
                }
            }
            while (!batch.endOfFile);
        }
        return rows;
    }

    private static String requiredEnv(String name)
    {
        String value = System.getenv(name);
        if (!nonEmpty(value))
        {
            fail("missing required environment variable: " + name);
        }
        return value.trim();
    }

    private static boolean nonEmpty(String value)
    {
        return value != null && !value.trim().isEmpty();
    }
}
