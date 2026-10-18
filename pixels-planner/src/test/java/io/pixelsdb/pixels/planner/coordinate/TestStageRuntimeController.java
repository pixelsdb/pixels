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
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.lease.Lease;
import io.pixelsdb.pixels.common.task.Task;
import io.pixelsdb.pixels.common.task.Worker;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestStageRuntimeController
{
    @Test(timeout = 3000)
    public void failedAttemptDoesNotWaitForConsumerMissingEnd() throws Exception
    {
        RecordingStageWorkerLauncher launcher = new RecordingStageWorkerLauncher();
        StageRuntimeController controller = controller(stageCoordinator(2), launcher);
        List<CompletableFuture<? extends Output>> attempts = controller.scaleTo(2);
        IllegalStateException original = new IllegalStateException("producer failed before END");
        attempts.get(0).completeExceptionally(original);
        try
        {
            controller.getExecutionHandle().getCompletionFuture().get(1, TimeUnit.SECONDS);
            fail("stage must fail");
        }
        catch (ExecutionException expected)
        {
            assertSame(original, expected.getCause());
        }
        assertFalse(attempts.get(1).isDone());
        controller.scaleTo(3);
        assertEquals(2, launcher.inputs.size());
    }

    @Test
    public void planCoordinatorExposesStageActivationAndScaling()
    {
        StageCoordinator stage = stageCoordinator(2);
        RecordingStageWorkerLauncher launcher = new RecordingStageWorkerLauncher();
        CoordinatorEndpoint endpoint = new CoordinatorEndpoint("coordinator.internal", 19000);
        PlanCoordinator planCoordinator = new PlanCoordinator(1277L, endpoint, launcher);
        planCoordinator.addStageCoordinator(stage, new StageDependency(17, -1, false));
        planCoordinator.addStageRuntimeController(new StageExecutionDescriptor(
                1277L, 1000L, 17, "s3qs-stage", WorkerType.PARTITION_S3QS, endpoint));

        assertEquals(2, planCoordinator.activateStage(17).size());
        assertEquals(2, launcher.inputs.size());
        assertEquals("coordinator.internal", launcher.inputs.get(0).getCoordinatorHost());
        assertEquals(19000, launcher.inputs.get(0).getCoordinatorPort());

        planCoordinator.scaleStage(17, 1);
        assertEquals(1, planCoordinator.getStageRuntimeController(17)
                .getStatus().getDesiredWorkerCount());
    }

    @Test
    public void scaleOutIsIdempotentAndLaunchesOnlyMissingCapacity()
    {
        StageCoordinator stage = stageCoordinator(3);
        RecordingStageWorkerLauncher launcher = new RecordingStageWorkerLauncher();
        StageRuntimeController controller = controller(stage, launcher);

        assertEquals(2, controller.scaleTo(2).size());
        assertEquals(2, launcher.inputs.size());
        assertEquals(2, controller.getStatus().getDesiredWorkerCount());
        assertEquals(2, controller.getStatus().getActiveAttemptCount());

        assertEquals(2, controller.scaleTo(2).size());
        assertEquals(2, launcher.inputs.size());

        assertEquals(3, controller.scaleTo(3).size());
        assertEquals(3, launcher.inputs.size());
        for (StageWorkerInput input : launcher.inputs)
        {
            assertEquals(17, input.getStageId());
            assertEquals(WorkerType.PARTITION_S3QS, input.getWorkerType());
        }
    }

    @Test
    public void scaleInDrainsRegisteredWorkerAndScaleOutLaunchesReplacement() throws Exception
    {
        StageCoordinator stage = stageCoordinator(3);
        RecordingStageWorkerLauncher launcher = new RecordingStageWorkerLauncher();
        StageRuntimeController controller = controller(stage, launcher);
        controller.scaleTo(2);

        Worker<CFWorkerInfo> first = worker(1L);
        Worker<CFWorkerInfo> second = worker(2L);
        stage.addWorker(first);
        stage.addWorker(second);
        assertEquals(2, stage.getAcceptingWorkerCount());

        controller.scaleTo(1);
        assertFalse(stage.isWorkerDraining(first.getWorkerId()));
        assertTrue(stage.isWorkerDraining(second.getWorkerId()));
        assertEquals(1, stage.getAcceptingWorkerCount());
        assertTrue(stage.getTasksToRun(second.getWorkerId()).isEmpty());

        controller.scaleTo(2);
        assertEquals(3, launcher.inputs.size());

        Worker<CFWorkerInfo> replacement = worker(3L);
        stage.addWorker(replacement);
        assertFalse(stage.isWorkerDraining(replacement.getWorkerId()));
        assertEquals(2, stage.getAcceptingWorkerCount());
        assertEquals(1, stage.getDrainingWorkerCount());
    }

    @Test
    public void drainingWorkerFinishesClaimedTaskButCannotClaimAnother() throws Exception
    {
        StageCoordinator stage = stageCoordinator(3);
        stage.setTaskBatchSize(1);
        StageRuntimeController controller = controller(stage, new RecordingStageWorkerLauncher());
        controller.scaleTo(2);
        Worker<CFWorkerInfo> first = worker(1L);
        Worker<CFWorkerInfo> second = worker(2L);
        stage.addWorker(first);
        stage.addWorker(second);

        List<Task> claimed = stage.getTasksToRun(second.getWorkerId());
        assertEquals(1, claimed.size());
        controller.scaleTo(1);
        assertTrue(stage.isWorkerDraining(second.getWorkerId()));
        assertFalse(second.isTerminated());
        assertTrue(stage.getTasksToRun(second.getWorkerId()).isEmpty());

        stage.completeTask(claimed.get(0).getTaskId(), true);
        assertEquals(1, stage.getCompletedTaskCount());
        assertFalse(second.isTerminated());
        assertEquals(1, stage.getTasksToRun(first.getWorkerId()).size());
    }

    @Test
    public void workerRegisteringAfterScaleToZeroCanNotClaimTasks() throws Exception
    {
        StageCoordinator stage = stageCoordinator(1);
        RecordingStageWorkerLauncher launcher = new RecordingStageWorkerLauncher();
        StageRuntimeController controller = controller(stage, launcher);

        controller.scaleTo(5);
        assertEquals(1, launcher.inputs.size());
        controller.scaleTo(0);

        Worker<CFWorkerInfo> lateWorker = new Worker<>(9L,
                new Lease(System.currentTimeMillis() - 10000L, 1L), 0,
                new CFWorkerInfo("localhost", -1, 1277L, 17,
                        Constants.PARTITION_OPERATOR_NAME, Collections.emptyList()));
        stage.addWorker(lateWorker);
        assertTrue(stage.isWorkerDraining(lateWorker.getWorkerId()));
        assertEquals(0, stage.getAcceptingWorkerCount());
        assertTrue(stage.getTasksToRun(lateWorker.getWorkerId()).isEmpty());
        assertEquals(1, stage.getPendingTaskCount());
    }

    @Test
    public void successfulWorkerOutputIsVisibleToOperator() throws Exception
    {
        StageCoordinator stage = stageCoordinator(1);
        RecordingStageWorkerLauncher launcher = new RecordingStageWorkerLauncher();
        StageRuntimeController controller = controller(stage, launcher);

        CompletableFuture<? extends Output> visibleAttempt = controller.scaleTo(1).get(0);
        PartitionOutput output = new PartitionOutput();
        output.setSuccessful(true);
        launcher.complete(0, output);

        assertSame(output, visibleAttempt.get());
        assertEquals(0, controller.getStatus().getActiveAttemptCount());
    }

    @Test
    public void unsuccessfulWorkerOutputFailsTheVisibleAttempt() throws Exception
    {
        StageCoordinator stage = stageCoordinator(1);
        RecordingStageWorkerLauncher launcher = new RecordingStageWorkerLauncher();
        StageRuntimeController controller = controller(stage, launcher);

        CompletableFuture<? extends Output> visibleAttempt = controller.scaleTo(1).get(0);
        PartitionOutput output = new PartitionOutput();
        output.setSuccessful(false);
        output.setErrorMessage("partition task failed");
        launcher.complete(0, output);

        try
        {
            visibleAttempt.get();
            fail("unsuccessful worker output should fail the stage attempt");
        }
        catch (ExecutionException e)
        {
            assertTrue(e.getCause() instanceof IllegalStateException);
            assertTrue(e.getCause().getMessage().contains("partition task failed"));
        }
        assertEquals(0, controller.getStatus().getActiveAttemptCount());
    }

    @Test
    public void autoScalingPreservesLogicalTasksAndCollectsLaterWorkerOutputs() throws Exception
    {
        StageCoordinator stage = stageCoordinator(6);
        RecordingStageWorkerLauncher launcher = new RecordingStageWorkerLauncher();
        CoordinatorEndpoint endpoint = new CoordinatorEndpoint("localhost", 18894);
        StageScalingConfig scalingConfig =
                new StageScalingConfig(true, 60000L, 1, 1, 8, 1, 4, 1, 3, 5000L);
        PlanCoordinator planCoordinator =
                new PlanCoordinator(1277L, endpoint, launcher, scalingConfig);
        planCoordinator.addStageCoordinator(stage, new StageDependency(17, -1, false));
        planCoordinator.addStageRuntimeController(new StageExecutionDescriptor(
                1277L, 1000L, 17, "s3qs-stage", WorkerType.PARTITION_S3QS, endpoint));

        StageExecutionHandle execution = planCoordinator.activateStageExecution(17);
        assertEquals(1, launcher.inputs.size());
        assertEquals(1, stage.getTaskBatchSize());

        StageScalingDecision scaleOut = planCoordinator.runStageScalingOnce(17, 1000L);
        assertEquals(StageScalingDecision.Direction.SCALE_OUT, scaleOut.getDirection());
        assertEquals(5, scaleOut.getTargetWorkerCount());
        assertEquals(5, launcher.inputs.size());

        List<Worker<CFWorkerInfo>> workers = new ArrayList<>();
        Set<Integer> claimedTaskIds = new HashSet<>();
        for (int i = 0; i < 5; ++i)
        {
            Worker<CFWorkerInfo> worker = worker(i + 1L);
            workers.add(worker);
            stage.addWorker(worker);
            List<Task> claimed = stage.getTasksToRun(worker.getWorkerId());
            assertEquals(1, claimed.size());
            assertTrue(claimedTaskIds.add(claimed.get(0).getTaskId()));
        }
        assertEquals(5, stage.getRunningTaskCount());
        assertEquals(1, stage.getPendingTaskCount());

        for (Integer taskId : new ArrayList<>(claimedTaskIds))
        {
            stage.completeTask(taskId, true);
        }
        List<Task> finalClaim = stage.getTasksToRun(workers.get(0).getWorkerId());
        assertEquals(1, finalClaim.size());
        assertTrue(claimedTaskIds.add(finalClaim.get(0).getTaskId()));
        stage.completeTask(finalClaim.get(0).getTaskId(), true);
        assertEquals(6, claimedTaskIds.size());
        assertTrue(stage.areLogicalTasksTerminal());

        planCoordinator.runStageScalingOnce(17, 2000L);
        for (int i = 0; i < launcher.futures.size(); ++i)
        {
            PartitionOutput output = new PartitionOutput();
            output.setSuccessful(true);
            output.setRequestId("attempt-" + i);
            launcher.complete(i, output);
        }

        List<Output> outputs = execution.getCompletionFuture().get(5, TimeUnit.SECONDS);
        assertEquals(5, outputs.size());
        for (int i = 0; i < outputs.size(); ++i)
        {
            assertEquals("attempt-" + i, outputs.get(i).getRequestId());
        }
        assertEquals(0, execution.getStatus().getDesiredWorkerCount());
        assertEquals(6, execution.getStatus().getCompletedTaskCount());
        planCoordinator.close();
    }

    private static StageRuntimeController controller(StageCoordinator stage,
                                                     RecordingStageWorkerLauncher launcher)
    {
        StageExecutionDescriptor descriptor = new StageExecutionDescriptor(
                1277L, 1000L, stage.getStageId(), "s3qs-stage", WorkerType.PARTITION_S3QS,
                new CoordinatorEndpoint("localhost", 18894));
        return new StageRuntimeController(stage, descriptor, launcher);
    }

    private static StageCoordinator stageCoordinator(int taskCount)
    {
        List<Task> tasks = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; ++i)
        {
            tasks.add(new Task(i, "task-" + i));
        }
        return new StageCoordinator(17, tasks);
    }

    private static Worker<CFWorkerInfo> worker(long workerId)
    {
        return new Worker<>(workerId, new Lease(System.currentTimeMillis(), 60000L), 0,
                new CFWorkerInfo("localhost", -1, 1277L, 17,
                        Constants.PARTITION_OPERATOR_NAME, Collections.emptyList()));
    }

    private static class RecordingStageWorkerLauncher implements StageWorkerLauncher
    {
        private final List<StageWorkerInput> inputs = new ArrayList<>();
        private final List<CompletableFuture<Output>> futures = new ArrayList<>();

        @Override
        public CompletableFuture<? extends Output> launch(WorkerType workerType, StageWorkerInput input)
        {
            inputs.add(input);
            CompletableFuture<Output> future = new CompletableFuture<>();
            futures.add(future);
            return future;
        }

        private void complete(int attemptIndex, Output output)
        {
            futures.get(attemptIndex).complete(output);
        }
    }
}
