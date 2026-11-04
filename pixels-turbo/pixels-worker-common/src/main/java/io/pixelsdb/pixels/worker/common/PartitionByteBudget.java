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

/** Invocation-local bytes, reserved before input IO / ownership transfer. */
final class PartitionByteBudget
{
    private final long limit;
    private long used, peak, waitNs;

    PartitionByteBudget(long limit)
    {
        if (limit <= 0) { throw new IllegalArgumentException("byte budget must be positive"); }
        this.limit = limit;
    }

    synchronized boolean tryAcquire(long bytes)
    {
        if (bytes < 0 || bytes > limit) { throw new WorkerException("single memory unit " + bytes + " exceeds byte budget " + limit); }
        if (used > limit - bytes) { return false; }
        used += bytes;
        peak = Math.max(peak, used);
        return true;
    }

    void acquire(long bytes, Runnable check)
    {
        long start = System.nanoTime();
        boolean waited = false;
        try
        {
            while (true)
            {
                check.run();
                synchronized (this)
                {
                    if (tryAcquire(bytes)) { return; }
                    waited = true;
                    wait(50);
                }
            }
        }
        catch (InterruptedException error)
        {
            Thread.currentThread().interrupt();
            throw new WorkerException("interrupted waiting for memory budget", error);
        }
        finally { if (waited) { synchronized (this) { waitNs += System.nanoTime() - start; } } }
    }

    synchronized void release(long bytes)
    {
        if (bytes < 0 || bytes > used) { throw new IllegalStateException("invalid memory reservation release " + bytes); }
        used -= bytes;
        notifyAll();
    }

    long limit() { return limit; }
    synchronized long used() { return used; }
    synchronized long peak() { return peak; }
    synchronized long waitNs() { return waitNs; }
}
