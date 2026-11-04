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
