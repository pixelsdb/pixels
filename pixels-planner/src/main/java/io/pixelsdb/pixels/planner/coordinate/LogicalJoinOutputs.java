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
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** Stable logical results while physical stage invocations scale dynamically. */
public final class LogicalJoinOutputs
{
    private final CompletableFuture<JoinOutput>[] results;
    private final Set<CompletableFuture<? extends Output>> delivered =
            Collections.newSetFromMap(new IdentityHashMap<>());
    private final Consumer<Throwable> abort;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private Throwable failure;

    private LogicalJoinOutputs(int count, Consumer<Throwable> abort)
    {
        if (count < 0) { throw new IllegalArgumentException("negative logical task count"); }
        this.abort = Objects.requireNonNull(abort, "abort is null");
        results = new CompletableFuture[count];
        for (int i = 0; i < count; i++) { results[i] = new CompletableFuture<>(); }
    }

    public static LogicalJoinOutputs create(StageExecutionHandle stage, int count,
                                                               Consumer<Throwable> abort)
    {
        LogicalJoinOutputs collector = new LogicalJoinOutputs(count, abort);
        stage.subscribeAttempts(attempt -> attempt.whenComplete(
                (output, failure) -> collector.accept(attempt, output, failure)));
        stage.getCompletionFuture().whenComplete((outputs, failure) ->
        {
            if (failure != null) { collector.fail(failure); return; }
            // CompletableFuture callback order is unspecified. Deliver every
            // completed attempt before checking for missing logical results.
            for (CompletableFuture<? extends Output> attempt : stage.getAttemptFutures())
            {
                collector.accept(attempt, attempt.join(), null);
            }
            collector.finish();
        });
        return collector;
    }

    public CompletableFuture<? extends Output>[] getOutputs() { return results.clone(); }
    public CompletableFuture<Void> getCompletionFuture() { return completion; }

    private synchronized void accept(CompletableFuture<? extends Output> attempt, Output output, Throwable error)
    {
        if (failure != null || !delivered.add(attempt)) { return; }
        if (error != null) { fail(error); return; }
        try
        {
            if (!(output instanceof JoinOutput) || !output.isSuccessful())
            {
                throw new IllegalStateException("S3QS join invocation failed: " +
                        (output == null ? "null output" : output.getErrorMessage()));
            }
            Map<Integer, JoinOutput> tasks = ((JoinOutput) output).getTaskOutputs();
            if (tasks == null)
            {
                if (!output.getOutputs().isEmpty())
                {
                    throw new IllegalStateException("S3QS worker response is missing logical task identities");
                }
                return; // An idle/drained invocation may execute no tasks.
            }
            for (Map.Entry<Integer, JoinOutput> task : tasks.entrySet())
            {
                int id = task.getKey();
                JoinOutput result = task.getValue();
                if (id < 0 || id >= results.length || results[id].isDone())
                {
                    throw new IllegalStateException("unexpected or duplicate logical join task " + id);
                }
                if (result == null || !result.isSuccessful())
                {
                    throw new IllegalStateException("logical join task " + id + " failed");
                }
            }
            // Validate the entire response before publishing any of its results.
            tasks.forEach((id, result) -> results[id].complete(result));
        }
        catch (Throwable cause) { fail(cause); }
    }

    private synchronized void finish()
    {
        if (failure != null) { return; }
        for (int i = 0; i < results.length; i++)
        {
            if (!results[i].isDone())
            {
                fail(new IllegalStateException("missing logical join task result " + i));
                return;
            }
        }
        completion.complete(null);
    }

    private synchronized void fail(Throwable cause)
    {
        if (failure != null) { return; }
        failure = QueryExecutionState.unwrap(cause);
        for (CompletableFuture<JoinOutput> result : results) { result.completeExceptionally(failure); }
        completion.completeExceptionally(failure);
        abort.accept(failure);
    }
}
