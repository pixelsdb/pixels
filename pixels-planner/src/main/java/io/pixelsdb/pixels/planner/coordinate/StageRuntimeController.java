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
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;

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
    private final StageExecutionHandle executionHandle;
    private final QueryExecutionState queryState;
    private int desiredWorkerCount;

    public StageRuntimeController(StageCoordinator stageCoordinator,
                                  StageExecutionDescriptor descriptor,
                                  StageWorkerLauncher workerLauncher)
    {
        this(stageCoordinator, descriptor, workerLauncher, new QueryExecutionState());
    }

    StageRuntimeController(StageCoordinator stageCoordinator, StageExecutionDescriptor descriptor,
                           StageWorkerLauncher workerLauncher, QueryExecutionState queryState)
    {
        this.queryState = requireNonNull(queryState, "queryState is null");
        this.stageCoordinator = requireNonNull(stageCoordinator, "stageCoordinator is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.workerLauncher = requireNonNull(workerLauncher, "workerLauncher is null");
        checkArgument(stageCoordinator.isQueued(), "runtime-controlled stage must be queued");
        checkArgument(stageCoordinator.getStageId() == descriptor.getStageId(),
                "stage coordinator and execution descriptor have different stage ids");
        this.desiredWorkerCount = 0;
        this.stageCoordinator.setDesiredRuntimeWorkerCount(0);
        this.executionHandle = new StageExecutionHandle(descriptor.getStageId(),
                this::getStatus, this::refreshCompletion);
    }

    /**
     * Change the desired physical capacity and return all currently active
     * platform invocation futures after applying the change.
     */
    public synchronized List<CompletableFuture<? extends Output>> scaleTo(int targetWorkerCount)
    {
        checkArgument(targetWorkerCount >= 0, "targetWorkerCount is negative");
        refreshCompletion();
        if (executionHandle.isLaunchesClosed() || queryState.getStatus() != QueryExecutionState.Status.RUNNING)
        {
            desiredWorkerCount = 0;
            stageCoordinator.setDesiredRuntimeWorkerCount(0);
            return Collections.unmodifiableList(new ArrayList<>(activeAttempts));
        }
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
            String invocationId;
            try
            {
                invocationId = queryState.reserveInvocation(descriptor.getStageId());
            }
            catch (java.util.concurrent.CancellationException cancelled)
            {
                break;
            }
            StageWorkerInput input = descriptor.createWorkerInput();
            input.setInvocationId(invocationId);
            CompletableFuture<? extends Output> launchedAttempt;
            try
            {
                launchedAttempt = requireNonNull(workerLauncher.launch(descriptor.getWorkerType(), input),
                        "worker launcher returned null future");
            }
            catch (Throwable failure)
            {
                queryState.invocationReturned(invocationId, false);
                queryState.abort(failure);
                executionHandle.closeLaunches(failure);
                throw failure;
            }
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
            executionHandle.addAttempt(attempt);
            visibleAttempts.add(attempt);
            attempt.whenComplete((output, error) ->
            {
                queryState.invocationReturned(invocationId, error == null);
                if (error != null)
                {
                    queryState.abort(error);
                }
                removeCompletedAttempt(attempt);
                refreshCompletion();
            });
        }
        return Collections.unmodifiableList(visibleAttempts);
    }

    public synchronized StageRuntimeStatus getStatus()
    {
        int activeRegisteredWorkerCount = stageCoordinator.getActiveRegisteredWorkerCount();
        int startingAttemptCount = Math.max(0, activeAttempts.size() - activeRegisteredWorkerCount);
        return new StageRuntimeStatus(desiredWorkerCount, activeAttempts.size(), startingAttemptCount,
                activeRegisteredWorkerCount,
                stageCoordinator.getAcceptingWorkerCount(),
                stageCoordinator.getDrainingWorkerCount(),
                stageCoordinator.getPendingTaskCount(),
                stageCoordinator.getRunningTaskCount(),
                stageCoordinator.getCompletedTaskCount(),
                stageCoordinator.getFailedTaskCount(),
                stageCoordinator.getTotalTaskCount(),
                stageCoordinator.areLogicalTasksTerminal(),
                System.currentTimeMillis());
    }

    public int getStageId()
    {
        return descriptor.getStageId();
    }

    public StageExecutionHandle getExecutionHandle()
    {
        return executionHandle;
    }

    synchronized void refreshCompletion()
    {
        if (!stageCoordinator.areLogicalTasksTerminal())
        {
            return;
        }
        desiredWorkerCount = 0;
        stageCoordinator.setDesiredRuntimeWorkerCount(0);
        int failedTaskCount = stageCoordinator.getFailedTaskCount();
        if (failedTaskCount > 0)
        {
            executionHandle.closeLaunches(new IllegalStateException(
                    "stage " + descriptor.getStageId() + " has " + failedTaskCount +
                            " failed logical task(s)"));
        }
        else
        {
            executionHandle.closeLaunches();
        }
    }

    private synchronized void removeCompletedAttempt(CompletableFuture<? extends Output> attempt)
    {
        activeAttempts.remove(attempt);
    }
}
