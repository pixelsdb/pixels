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
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Stable stage execution view that includes attempts launched after activation.
 */
public class StageExecutionHandle
{
    private final int stageId;
    private final Supplier<StageRuntimeStatus> statusSupplier;
    private final Runnable attemptCompletionListener;
    private final List<CompletableFuture<? extends Output>> attemptFutures = new ArrayList<>();
    private final List<Output> outputs = new ArrayList<>();
    private final CompletableFuture<List<Output>> completionFuture = new CompletableFuture<>();
    private int activeAttemptCount;
    private Throwable firstFailure;
    private boolean launchesClosed;

    StageExecutionHandle(int stageId, Supplier<StageRuntimeStatus> statusSupplier,
                         Runnable attemptCompletionListener)
    {
        this.stageId = stageId;
        this.statusSupplier = requireNonNull(statusSupplier, "statusSupplier is null");
        this.attemptCompletionListener =
                requireNonNull(attemptCompletionListener, "attemptCompletionListener is null");
    }

    synchronized void addAttempt(CompletableFuture<? extends Output> attempt)
    {
        checkState(!launchesClosed, "stage %s no longer accepts worker attempts", stageId);
        int attemptIndex = attemptFutures.size();
        attemptFutures.add(requireNonNull(attempt, "attempt is null"));
        outputs.add(null);
        activeAttemptCount++;
        attempt.whenComplete((output, failure) ->
        {
            synchronized (StageExecutionHandle.this)
            {
                activeAttemptCount--;
                if (failure == null)
                {
                    outputs.set(attemptIndex, output);
                }
                else if (firstFailure == null)
                {
                    firstFailure = failure;
                }
                tryComplete();
            }
            attemptCompletionListener.run();
        });
    }

    synchronized void closeLaunches()
    {
        launchesClosed = true;
        tryComplete();
    }

    synchronized void closeLaunches(Throwable failure)
    {
        if (firstFailure == null)
        {
            firstFailure = requireNonNull(failure, "failure is null");
        }
        closeLaunches();
    }

    public int getStageId()
    {
        return stageId;
    }

    public CompletableFuture<List<Output>> getCompletionFuture()
    {
        return completionFuture;
    }

    public StageRuntimeStatus getStatus()
    {
        return statusSupplier.get();
    }

    public synchronized List<CompletableFuture<? extends Output>> getAttemptFutures()
    {
        return Collections.unmodifiableList(new ArrayList<>(attemptFutures));
    }

    synchronized int getActiveAttemptCount()
    {
        return activeAttemptCount;
    }

    synchronized boolean isLaunchesClosed()
    {
        return launchesClosed;
    }

    private void tryComplete()
    {
        if (firstFailure != null)
        {
            launchesClosed = true;
            completionFuture.completeExceptionally(QueryExecutionState.unwrap(firstFailure));
            return;
        }
        if (!launchesClosed || activeAttemptCount != 0 || completionFuture.isDone())
        {
            return;
        }
        if (firstFailure != null)
        {
            completionFuture.completeExceptionally(firstFailure);
            return;
        }
        completionFuture.complete(Collections.unmodifiableList(new ArrayList<>(outputs)));
    }
}
