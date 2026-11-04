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

import java.util.LinkedHashMap;
import java.util.Map;

/** Invocation-local service-cost feedback; byte admission remains owned by the two memory pools. */
final class PartitionPipeline
{
    private final int maxReaders, writers;
    private int readers, active, peak, scanSamples, writeSamples, warmupWindows;
    private double scanCost, writeCost;
    private long scanBytes, scanNs, writeBytes, writeNs, schedulingNs;

    PartitionPipeline(int maxReaders, int writers)
    {
        this.maxReaders = Math.max(1, maxReaders);
        this.writers = writers;
        readers = Math.min(this.maxReaders, Math.max(1, (writers + 1) / 2));
    }

    void acquire(Runnable check)
    {
        long begin = System.nanoTime();
        boolean waited = false;
        try
        {
            while (true)
            {
                check.run();
                synchronized (this)
                {
                    if (active < readers) { active++; peak = Math.max(peak, active); return; }
                    waited = true;
                    wait(50);
                }
            }
        }
        catch (InterruptedException error)
        {
            Thread.currentThread().interrupt();
            throw new WorkerException("interrupted waiting for scan concurrency", error);
        }
        finally { if (waited) { synchronized (this) { schedulingNs += System.nanoTime() - begin; } } }
    }

    synchronized void release() { active--; notifyAll(); }

    synchronized void scanned(long bytes, long nanos)
    {
        scanBytes += bytes; scanNs += nanos;
        // Before the first full output group, cautiously fill the pipeline.
        // A window may emit no detached batch simply because partition tails
        // are still accumulating; that does not imply a fully filtered scan.
        if (writeCost == 0 && ++warmupWindows >= 4)
        {
            readers = Math.min(maxReaders, readers + 1);
            warmupWindows = 0;
            notifyAll();
        }
        // Tiny samples are dominated by setup and tail effects.
        if (bytes < 65536 || nanos <= 0) { return; }
        scanCost = smooth(scanCost, nanos / (double) bytes);
        scanSamples++;
        adjust();
    }

    synchronized void written(long bytes, long nanos)
    {
        writeBytes += bytes; writeNs += nanos;
        if (bytes < 65536 || nanos <= 0) { return; }
        writeCost = smooth(writeCost, nanos / (double) bytes);
        writeSamples++;
        adjust();
    }

    private static double smooth(double previous, double value)
    {
        return previous == 0 ? value : previous * 0.75 + value * 0.25;
    }

    private void adjust()
    {
        if (scanSamples < 4 || writeSamples < 2) { return; }
        // Equal raw-output-byte rates: R / scanCost ~= W / writeCost.
        int target = (int) Math.max(1, Math.min(maxReaders, Math.ceil(writers * scanCost / writeCost)));
        // Change one slot at a time after fresh samples from both stages.
        readers += Integer.compare(target, readers);
        scanSamples = writeSamples = 0;
        notifyAll();
    }

    synchronized Map<String, Long> snapshot()
    {
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("writerThreads", (long) writers);
        metrics.put("scanConcurrencyTarget", (long) readers);
        metrics.put("scanConcurrencyPeak", (long) peak);
        metrics.put("scanSchedulingWaitNs", schedulingNs);
        metrics.put("scanServiceBytes", scanBytes); metrics.put("scanServiceNs", scanNs);
        metrics.put("writeServiceBytes", writeBytes); metrics.put("writeServiceNs", writeNs);
        return metrics;
    }
}
