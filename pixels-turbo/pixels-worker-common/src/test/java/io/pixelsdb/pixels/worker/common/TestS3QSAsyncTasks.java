/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import org.junit.Test;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;

public class TestS3QSAsyncTasks
{
    @Test(timeout = 5000)
    public void successHookRunsAfterCleanupAndItsFailureReachesTheBarrier() throws Exception
    {
        AtomicInteger released = new AtomicInteger();
        IOException original = new IOException("replenishment failed");
        try (S3QSAsyncTasks tasks = new S3QSAsyncTasks(new WorkerCancellation(), 1))
        {
            tasks.submit(() -> {}, released::incrementAndGet, () -> {
                assertEquals(1, released.get()); throw original;
            });
            assertSame(original, assertThrows(IOException.class, tasks::await));
        }
        assertEquals(1, released.get());
    }

    @Test(timeout = 5000)
    public void failedWorkReleasesCreditWithoutCallingSuccessHook() throws Exception
    {
        AtomicInteger released = new AtomicInteger(), replenished = new AtomicInteger();
        IOException original = new IOException("PUT failed");
        try (S3QSAsyncTasks tasks = new S3QSAsyncTasks(new WorkerCancellation(), 1))
        {
            tasks.submit(() -> { throw original; }, released::incrementAndGet, replenished::incrementAndGet);
            assertSame(original, assertThrows(IOException.class, tasks::await));
        }
        assertEquals(1, released.get()); assertEquals(0, replenished.get());
    }

    @Test(timeout = 5000)
    public void consumerAckLimitAppliesBackpressureWithoutRunningIOOnCaller() throws Exception
    {
        CountDownLatch running = new CountDownLatch(2), release = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger(), peak = new AtomicInteger();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        Thread main = Thread.currentThread();
        try (S3QSAsyncTasks tasks = new S3QSAsyncTasks(new WorkerCancellation(), 4, 2))
        {
            try
            {
                for (int i = 0; i < 2; ++i)
                {
                    tasks.submit(() -> {
                        assertNotSame(main, Thread.currentThread());
                        peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                        running.countDown();
                        try { release.await(); }
                        finally { active.decrementAndGet(); }
                    });
                }
                assertTrue(running.await(1, TimeUnit.SECONDS));
                Future<?> third = caller.submit(() -> tasks.submit(() -> {}));
                assertThrows(TimeoutException.class, () -> third.get(100, TimeUnit.MILLISECONDS));
                release.countDown();
                third.get(1, TimeUnit.SECONDS);
                tasks.await();
                assertEquals(2, peak.get());
            }
            finally { release.countDown(); }
        }
        finally { caller.shutdownNow(); assertTrue(caller.awaitTermination(1, TimeUnit.SECONDS)); }
    }

    @Test(timeout = 5000)
    public void barrierPropagatesOriginalFailureAndStopsOwnedChildren() throws Exception
    {
        WorkerCancellation cancellation = new WorkerCancellation();
        IOException original = new IOException("injected async failure");
        try (S3QSAsyncTasks tasks = new S3QSAsyncTasks(cancellation, 2))
        {
            tasks.submit(() -> { throw original; });
            assertSame(original, assertThrows(IOException.class, tasks::await));
        }
        assertTrue(cancellation.isQuiescent());
    }

    @Test(timeout = 5000)
    public void barrierReportsFailureWithoutWaitingForAnotherUninterruptibleIO() throws Exception
    {
        CountDownLatch running = new CountDownLatch(1), release = new CountDownLatch(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        IOException original = new IOException("first I/O failed");
        try (S3QSAsyncTasks tasks = new S3QSAsyncTasks(new WorkerCancellation(), 2))
        {
            try
            {
                tasks.submit(() -> {
                    running.countDown();
                    while (release.getCount() != 0)
                    {
                        try { release.await(); }
                        catch (InterruptedException ignored) { /* Model an SDK call that ignores interrupts. */ }
                    }
                });
                assertTrue(running.await(1, TimeUnit.SECONDS));
                tasks.submit(() -> { throw original; });
                Future<?> barrier = caller.submit(() -> { tasks.await(); return null; });
                ExecutionException failure = assertThrows(ExecutionException.class, () -> barrier.get(1, TimeUnit.SECONDS));
                assertSame(original, failure.getCause());
            }
            finally { release.countDown(); }
        }
        finally { caller.shutdownNow(); assertTrue(caller.awaitTermination(1, TimeUnit.SECONDS)); }
    }

    @Test(timeout = 5000)
    public void cancellationUnblocksSubmissionAndDrainsBackgroundWork() throws Exception
    {
        WorkerCancellation cancellation = new WorkerCancellation();
        CountDownLatch running = new CountDownLatch(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (S3QSAsyncTasks tasks = new S3QSAsyncTasks(cancellation, 1, 1))
        {
            tasks.submit(() -> { running.countDown(); new CountDownLatch(1).await(); });
            assertTrue(running.await(1, TimeUnit.SECONDS));
            Future<?> blocked = caller.submit(() -> tasks.submit(() -> fail("cancelled task ran")));
            cancellation.cancel(new IOException("cancelled"));
            assertThrows(ExecutionException.class, () -> blocked.get(1, TimeUnit.SECONDS));
        }
        finally { caller.shutdownNow(); assertTrue(caller.awaitTermination(1, TimeUnit.SECONDS)); }
        assertTrue(cancellation.isQuiescent());
    }
}
