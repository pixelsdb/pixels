/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.planner.coordinate;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import org.junit.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.Assert.*;

public class TestLogicalJoinOutputs
{
    @Test
    public void dynamicInvocationsReturnStableTaskOrderAndMultipleFiles() throws Exception
    {
        StageExecutionHandle stage = stage();
        CompletableFuture<Output> first = new CompletableFuture<>();
        stage.addAttempt(first);
        JoinOutput task2 = task("task2-main", "task2-outer");
        first.complete(response(2, task2)); // Completed before subscription.
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CompletableFuture<? extends Output>[] logical = LogicalJoinOutputs.create(stage, 3, failure::set).getOutputs();
        assertSame(task2, logical[2].get());
        assertFalse(logical[0].isDone());
        CompletableFuture<Output> later = new CompletableFuture<>();
        stage.addAttempt(later); // Admitted after the initial execute() snapshot.
        JoinOutput task0 = task("task0"), task1 = task("task1");
        JoinOutput aggregate = response(1, task1);
        aggregate.addTaskOutput(0, task0);
        later.complete(aggregate); // One invocation executes multiple tasks.
        stage.closeLaunches();
        assertSame(task0, logical[0].get());
        assertSame(task1, logical[1].get());
        assertEquals(Arrays.asList("task2-main", "task2-outer"), logical[2].get().getOutputs());
        assertNull(failure.get());
    }

    @Test
    public void emptyTaskOutputIsStillACompletedLogicalTask() throws Exception
    {
        StageExecutionHandle stage = stage();
        CompletableFuture<? extends Output>[] logical = LogicalJoinOutputs.create(stage, 1, cause -> fail(cause.toString())).getOutputs();
        stage.addAttempt(CompletableFuture.completedFuture(response(0, task())));
        stage.addAttempt(CompletableFuture.completedFuture(task())); // Idle worker.
        stage.closeLaunches();
        assertEquals(0, logical[0].get().toSimpleOutput().getNumOutputs());
    }

    @Test
    public void jsonRoundTripPreservesTaskIdsAndExactOutputs()
    {
        JoinOutput original = response(7, task("main", "outer"));
        JoinOutput decoded = JSON.parseObject(JSON.toJSONString(original), JoinOutput.class);
        assertEquals(Arrays.asList("main", "outer"), decoded.getTaskOutputs().get(7).getOutputs());
        assertTrue(decoded.getTaskOutputs().get(7).isSuccessful());
    }

    @Test
    public void failureAndCancellationCompleteOutstandingLogicalResults() throws Exception
    {
        for (boolean cancel : Arrays.asList(false, true))
        {
            StageExecutionHandle stage = stage();
            AtomicReference<Throwable> cause = new AtomicReference<>();
            CompletableFuture<? extends Output>[] logical = LogicalJoinOutputs.create(stage, 2, cause::set).getOutputs();
            CompletableFuture<Output> attempt = new CompletableFuture<>();
            stage.addAttempt(attempt);
            if (cancel) { attempt.cancel(false); }
            else { attempt.completeExceptionally(new RuntimeException("platform OOM")); }
            assertTrue(logical[0].isCompletedExceptionally());
            assertTrue(logical[1].isCompletedExceptionally());
            assertNotNull(cause.get());
        }
    }

    @Test
    public void missingUnexpectedDuplicateAndLegacyResultsAbortQuery()
    {
        for (String invalid : Arrays.asList("missing", "unexpected", "duplicate", "legacy", "unsuccessful"))
        {
            StageExecutionHandle stage = stage();
            AtomicReference<Throwable> cause = new AtomicReference<>();
            CompletableFuture<? extends Output>[] logical = LogicalJoinOutputs.create(stage, 2, cause::set).getOutputs();
            if (invalid.equals("unexpected"))
            {
                stage.addAttempt(CompletableFuture.completedFuture(response(9, task("bad"))));
            }
            else if (invalid.equals("legacy"))
            {
                stage.addAttempt(CompletableFuture.completedFuture(task("no task identity")));
            }
            else if (invalid.equals("unsuccessful"))
            {
                JoinOutput failed = task(); failed.setSuccessful(false);
                stage.addAttempt(CompletableFuture.completedFuture(failed));
            }
            else if (invalid.equals("duplicate"))
            {
                stage.addAttempt(CompletableFuture.completedFuture(response(0, task("a"))));
                stage.addAttempt(CompletableFuture.completedFuture(response(0, task("a"))));
            }
            stage.closeLaunches();
            assertNotNull(invalid, cause.get());
            assertTrue(invalid, logical[1].isCompletedExceptionally());
        }
    }

    private static StageExecutionHandle stage()
    {
        return new StageExecutionHandle(1, () -> null, () -> {});
    }

    private static JoinOutput task(String... paths)
    {
        JoinOutput output = new JoinOutput();
        output.setSuccessful(true);
        output.setOutputs(new ArrayList<>(Arrays.asList(paths)));
        return output;
    }

    private static JoinOutput response(int id, JoinOutput task)
    {
        JoinOutput output = task();
        output.addTaskOutput(id, task);
        output.getOutputs().addAll(task.getOutputs());
        return output;
    }
}
