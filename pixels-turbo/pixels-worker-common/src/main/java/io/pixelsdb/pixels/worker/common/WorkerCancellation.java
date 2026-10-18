/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Explicit invocation-local cancellation shared with all owned child tasks. */
public final class WorkerCancellation
{
    private final boolean enabled;
    private final AtomicReference<Throwable> cause = new AtomicReference<>();
    private final Set<ExecutorService> executors = ConcurrentHashMap.newKeySet();

    public WorkerCancellation() { this(true); }
    private WorkerCancellation(boolean enabled) { this.enabled = enabled; }
    public static WorkerCancellation disabled() { return new WorkerCancellation(false); }
    public boolean isEnabled() { return enabled; }

    public void cancel(Throwable failure)
    {
        java.util.Objects.requireNonNull(failure, "failure is null");
        if (enabled && cause.compareAndSet(null, failure))
        {
            executors.forEach(ExecutorService::shutdownNow);
        }
    }

    public void check()
    {
        Throwable failure = cause.get();
        if (enabled && failure != null) { throw new WorkerException("S3QS invocation cancelled", failure); }
    }

    void own(ExecutorService executor)
    {
        if (enabled)
        {
            executors.add(executor);
            if (cause.get() != null) { executor.shutdownNow(); }
        }
    }

    public boolean isQuiescent()
    {
        return executors.stream().allMatch(ExecutorService::isTerminated);
    }

    /** A bounded drain is evidence only when every executor actually terminated. */
    public boolean stopAndAwait(long timeoutMs)
    {
        executors.forEach(ExecutorService::shutdownNow);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        boolean interrupted = false;
        for (ExecutorService executor : executors)
        {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) { break; }
            try { executor.awaitTermination(remaining, TimeUnit.NANOSECONDS); }
            catch (InterruptedException failure) { interrupted = true; break; }
        }
        if (interrupted) { Thread.currentThread().interrupt(); }
        return isQuiescent();
    }
}
