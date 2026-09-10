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

import io.pixelsdb.pixels.common.turbo.Output;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Coordinator-owned control plane for physical worker attempts of one stage.
 *
 * StageCoordinator remains authoritative for logical tasks and registered
 * workers. This class owns desired capacity and platform invocation futures.
 */
public class StageRuntimeController
{
    private final StageCoordinator stageCoordinator;
    private final StageExecutionDescriptor descriptor;
    private final StageWorkerLauncher workerLauncher;
    private final List<CompletableFuture<? extends Output>> activeAttempts = new ArrayList<>();
    private int desiredWorkerCount;

    public StageRuntimeController(StageCoordinator stageCoordinator,
                                  StageExecutionDescriptor descriptor,
                                  StageWorkerLauncher workerLauncher)
    {
        this.stageCoordinator = requireNonNull(stageCoordinator, "stageCoordinator is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.workerLauncher = requireNonNull(workerLauncher, "workerLauncher is null");
        checkArgument(stageCoordinator.isQueued(), "runtime-controlled stage must be queued");
        checkArgument(stageCoordinator.getStageId() == descriptor.getStageId(),
                "stage coordinator and execution descriptor have different stage ids");
        this.desiredWorkerCount = 0;
        this.stageCoordinator.setDesiredRuntimeWorkerCount(0);
    }

    /**
     * Change the desired physical capacity and return all currently active
     * platform invocation futures after applying the change.
     */
    public synchronized List<CompletableFuture<? extends Output>> scaleTo(int targetWorkerCount)
    {
        checkArgument(targetWorkerCount >= 0, "targetWorkerCount is negative");
        desiredWorkerCount = targetWorkerCount;
        stageCoordinator.setDesiredRuntimeWorkerCount(targetWorkerCount);

        int unregisteredAttemptCount = Math.max(0,
                activeAttempts.size() - stageCoordinator.getActiveRegisteredWorkerCount());
        int effectiveWorkerCount = stageCoordinator.getAcceptingWorkerCount() + unregisteredAttemptCount;
        int workersToLaunch = Math.max(0, targetWorkerCount - effectiveWorkerCount);

        // A physical worker can only help with pending work. Running tasks are
        // deliberately not migrated during scale-out.
        workersToLaunch = Math.min(workersToLaunch, stageCoordinator.getPendingTaskCount());

        List<CompletableFuture<? extends Output>> visibleAttempts = new ArrayList<>(activeAttempts);
        for (int i = 0; i < workersToLaunch; ++i)
        {
            CompletableFuture<? extends Output> launchedAttempt = requireNonNull(
                    workerLauncher.launch(descriptor.getWorkerType(), descriptor.createWorkerInput()),
                    "worker launcher returned null future");
            CompletableFuture<? extends Output> attempt = launchedAttempt.thenApply(output ->
            {
                if (output == null)
                {
                    throw new CompletionException(new IllegalStateException(
                            "runtime worker returned null output for stage " + descriptor.getStageId()));
                }
                if (!output.isSuccessful())
                {
                    String errorMessage = output.getErrorMessage();
                    throw new CompletionException(new IllegalStateException(
                            "runtime worker failed for stage " + descriptor.getStageId() +
                                    (errorMessage == null || errorMessage.isEmpty() ? "" : ": " + errorMessage)));
                }
                return output;
            });
            activeAttempts.add(attempt);
            visibleAttempts.add(attempt);
            attempt.whenComplete((output, error) -> removeCompletedAttempt(attempt));
        }
        return Collections.unmodifiableList(visibleAttempts);
    }

    public synchronized StageRuntimeStatus getStatus()
    {
        return new StageRuntimeStatus(desiredWorkerCount, activeAttempts.size(),
                stageCoordinator.getActiveRegisteredWorkerCount(),
                stageCoordinator.getAcceptingWorkerCount(),
                stageCoordinator.getDrainingWorkerCount(),
                stageCoordinator.getPendingTaskCount(),
                stageCoordinator.getRunningTaskCount(),
                stageCoordinator.getCompletedTaskCount(),
                stageCoordinator.getFailedTaskCount());
    }

    public int getStageId()
    {
        return descriptor.getStageId();
    }

    private synchronized void removeCompletedAttempt(CompletableFuture<? extends Output> attempt)
    {
        activeAttempts.remove(attempt);
    }
}
