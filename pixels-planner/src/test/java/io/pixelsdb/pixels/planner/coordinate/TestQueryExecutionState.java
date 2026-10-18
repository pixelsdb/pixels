/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.planner.coordinate;

import org.junit.Test;
import java.util.concurrent.*;
import static org.junit.Assert.*;

public class TestQueryExecutionState
{
    @Test(timeout = 5000)
    public void concurrentFailuresKeepOneCauseAndCloseAdmission() throws Exception
    {
        QueryExecutionState state = new QueryExecutionState();
        RuntimeException first = new RuntimeException("producer");
        RuntimeException second = new RuntimeException("consumer");
        CountDownLatch start = new CountDownLatch(1);
        CompletableFuture<Boolean> a = CompletableFuture.supplyAsync(() -> failAt(start, state, first));
        CompletableFuture<Boolean> b = CompletableFuture.supplyAsync(() -> failAt(start, state, second));
        start.countDown();
        assertTrue(a.get() ^ b.get());
        assertSame(a.get() ? first : second, state.getFirstFailure());
        assertFalse(state.succeed());
        try { state.reserveInvocation(1); fail("abort must close launch admission"); }
        catch (CancellationException expected) { assertSame(state.getFirstFailure(), expected.getCause()); }
    }

    @Test
    public void exceptionalInvocationDoesNotProveRemoteQuiescence()
    {
        QueryExecutionState state = new QueryExecutionState();
        String invocation = state.reserveInvocation(3);
        assertTrue(state.registerInvocation(invocation, 3, 42));
        state.invocationReturned(invocation, false);
        state.abort(new TimeoutException("RPC deadline"));
        assertFalse(state.isQuiescent());
        assertFalse(state.getQuiescenceFuture().isDone());
        state.workerStopped(invocation, 99);
        assertFalse(state.isQuiescent());
        state.workerStopped(invocation, 42);
        assertTrue(state.isQuiescent());
        assertTrue(state.getQuiescenceFuture().isDone());
    }

    @Test
    public void lateAndDuplicateRegistrationsCannotObtainTasks()
    {
        QueryExecutionState state = new QueryExecutionState();
        String id = state.reserveInvocation(3);
        assertFalse(state.registerInvocation(id, 4, 1));
        assertTrue(state.registerInvocation(id, 3, 1));
        assertFalse(state.registerInvocation(id, 3, 2));
        String late = state.reserveInvocation(3);
        state.abort(new RuntimeException("cancel"));
        assertFalse(state.registerInvocation(late, 3, 3));
        assertFalse(state.isQuiescent());
    }

    private static boolean failAt(CountDownLatch latch, QueryExecutionState state, Throwable failure)
    {
        try { latch.await(); return state.abort(failure); }
        catch (InterruptedException e) { throw new AssertionError(e); }
    }
}
