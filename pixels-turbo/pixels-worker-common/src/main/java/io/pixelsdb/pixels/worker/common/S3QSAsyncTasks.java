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
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.concurrent.ExecutorService;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Invocation-owned I/O tasks. No work is run on the submitting thread. */
final class S3QSAsyncTasks implements AutoCloseable
{
    interface Task { void run() throws Exception; }

    private final WorkerCancellation cancellation;
    private final ExecutorService executor;
    private final Semaphore slots;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean closed;

    /** Producer admission is controlled by the output byte budget, without a job-count limit. */
    S3QSAsyncTasks(WorkerCancellation cancellation, int threads)
    {
        this(cancellation, threads, null);
    }

    /** Consumer acknowledgements retain their separate bound on queued and running jobs. */
    S3QSAsyncTasks(WorkerCancellation cancellation, int threads, int maxInFlight)
    {
        this(cancellation, Math.min(threads, maxInFlight), new Semaphore(maxInFlight));
    }

    private S3QSAsyncTasks(WorkerCancellation cancellation, int threads, Semaphore slots)
    {
        if (threads <= 0)
        {
            throw new IllegalArgumentException("S3QS I/O concurrency must be positive");
        }
        this.cancellation = cancellation;
        this.slots = slots;
        this.executor = new WorkerThreadPool(threads, java.util.concurrent.Executors.defaultThreadFactory())
        {
            @Override
            public List<Runnable> shutdownNow()
            {
                List<Runnable> pending = super.shutdownNow();
                for (Runnable job : pending) { ((Job) job).release(); }
                return pending;
            }
        };
        cancellation.own(executor);
    }

    static int setting(String name, int fallback)
    {
        String value = ConfigFactory.Instance().getProperty(name);
        int result = value == null ? fallback : Integer.parseInt(value);
        if (result <= 0) { throw new IllegalArgumentException(name + " must be positive"); }
        return result;
    }

    void check()
    {
        Throwable error = failure.get();
        if (error instanceof RuntimeException) { throw (RuntimeException) error; }
        if (error instanceof Error) { throw (Error) error; }
        if (error != null) { throw new WorkerException("asynchronous S3QS I/O failed", error); }
        cancellation.check();
        if (closed) { throw new WorkerException("S3QS I/O scope is closed"); }
    }

    void submit(Task task) { submit(task, () -> {}); }

    /** Cleanup runs exactly once, after real execution or removal from the queue. */
    void submit(Task task, Runnable cleanup)
    {
        submit(task, cleanup, () -> {});
    }

    /** Successful work can replenish its queue after releasing its admission credit. */
    void submit(Task task, Runnable cleanup, Task afterSuccess)
    {
        boolean acquired = false;
        try
        {
            check();
            if (slots != null)
            {
                while (!slots.tryAcquire(100, TimeUnit.MILLISECONDS)) { check(); }
                acquired = true;
                check();
            }
            executor.execute(new Job(task, cleanup, afterSuccess));
        }
        catch (InterruptedException error)
        {
            if (acquired) { slots.release(); }
            cleanup.run();
            Thread.currentThread().interrupt();
            throw new WorkerException("interrupted submitting S3QS I/O", error);
        }
        catch (RuntimeException | Error error)
        {
            if (acquired) { slots.release(); }
            cleanup.run();
            check();
            throw error;
        }
    }

    private final class Job implements Runnable
    {
        private final Task task;
        private final Runnable cleanup;
        private final Task afterSuccess;
        private final AtomicBoolean released = new AtomicBoolean();
        Job(Task task, Runnable cleanup, Task afterSuccess)
        {
            this.task = task; this.cleanup = cleanup; this.afterSuccess = afterSuccess;
        }
        @Override
        public void run()
        {
            try { check(); task.run(); release(); afterSuccess.run(); }
            catch (Throwable error)
            {
                if (failure.compareAndSet(null, error)) { executor.shutdownNow(); }
            }
            finally { release(); }
        }
        void release()
        {
            if (released.compareAndSet(false, true))
            {
                try { cleanup.run(); }
                finally { if (slots != null) { slots.release(); } }
            }
        }
    }

    /** Barrier: callers must not publish END or report task success before this returns. */
    void await() throws Exception
    {
        executor.shutdown();
        while (!executor.awaitTermination(100, TimeUnit.MILLISECONDS))
        {
            rethrowFailure();
            cancellation.check();
        }
        rethrowFailure();
        check();
    }

    private void rethrowFailure() throws Exception
    {
        Throwable error = failure.get();
        if (error instanceof Exception) { throw (Exception) error; }
        if (error instanceof Error) { throw (Error) error; }
    }

    void afterTermination(Runnable cleanup) { ((WorkerThreadPool) executor).afterTermination(cleanup); }

    @Override
    public void close()
    {
        closed = true;
        executor.shutdownNow();
        boolean interrupted = Thread.interrupted();
        try
        {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS))
            {
                throw new WorkerException("S3QS I/O tasks did not terminate during cleanup");
            }
        }
        catch (InterruptedException error)
        {
            interrupted = true;
            throw new WorkerException("interrupted draining S3QS I/O tasks", error);
        }
        finally { if (interrupted) { Thread.currentThread().interrupt(); } }
    }
}
