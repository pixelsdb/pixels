/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Invocation-owned, bounded I/O tasks. No work is run on the submitting thread. */
final class S3QSAsyncTasks implements AutoCloseable
{
    interface Task { void run() throws Exception; }

    private final WorkerCancellation cancellation;
    private final ExecutorService executor;
    private final Semaphore slots;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean closed;

    S3QSAsyncTasks(WorkerCancellation cancellation, int threads, int maxInFlight)
    {
        if (threads <= 0 || maxInFlight <= 0)
        {
            throw new IllegalArgumentException("S3QS I/O concurrency and in-flight limit must be positive");
        }
        this.cancellation = cancellation;
        this.slots = new Semaphore(maxInFlight);
        this.executor = Executors.newFixedThreadPool(Math.min(threads, maxInFlight));
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

    void submit(Task task)
    {
        check();
        try
        {
            while (!slots.tryAcquire(100, TimeUnit.MILLISECONDS)) { check(); }
        }
        catch (InterruptedException error)
        {
            Thread.currentThread().interrupt();
            throw new WorkerException("interrupted submitting S3QS I/O", error);
        }
        try
        {
            check();
            executor.execute(() -> {
                try { check(); task.run(); }
                catch (Throwable error)
                {
                    if (failure.compareAndSet(null, error)) { executor.shutdownNow(); }
                }
                finally { slots.release(); }
            });
        }
        catch (RuntimeException | Error error)
        {
            slots.release();
            check();
            throw error;
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
