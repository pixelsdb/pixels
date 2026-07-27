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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestStageRuntimeController
{
    @Test
    public void planCoordinatorExposesStageActivationAndScaling()
    {
        StageCoordinator stage = stageCoordinator(2);
        RecordingStageWorkerLauncher launcher = new RecordingStageWorkerLauncher();
        PlanCoordinator planCoordinator = new PlanCoordinator(1277L, launcher);
        planCoordinator.addStageCoordinator(stage, new StageDependency(17, -1, false));
        planCoordinator.addStageRuntimeController(new StageExecutionDescriptor(
                1277L, 1000L, 17, "s3qs-stage", WorkerType.PARTITION_S3QS));

        assertEquals(2, planCoordinator.activateStage(17).size());
        assertEquals(2, launcher.inputs.size());

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

    private static StageRuntimeController controller(StageCoordinator stage,
                                                     RecordingStageWorkerLauncher launcher)
    {
        StageExecutionDescriptor descriptor = new StageExecutionDescriptor(
                1277L, 1000L, stage.getStageId(), "s3qs-stage", WorkerType.PARTITION_S3QS);
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
        private final List<CompletableFuture<? extends Output>> futures = new ArrayList<>();

        @Override
        public CompletableFuture<? extends Output> launch(WorkerType workerType, StageWorkerInput input)
        {
            inputs.add(input);
            CompletableFuture<Output> future = new CompletableFuture<>();
            futures.add(future);
            return future;
        }
    }
}
