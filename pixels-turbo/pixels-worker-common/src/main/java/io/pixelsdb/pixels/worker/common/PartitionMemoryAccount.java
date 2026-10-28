/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionMemoryPolicy;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/** Separate input/output pools leave progress space for the writer and runtime. */
final class PartitionMemoryAccount
{
    final PartitionMemoryPolicy policy;
    final PartitionByteBudget input, output;
    private final AtomicLong windows = new AtomicLong();
    private final AtomicLong maxGroups = new AtomicLong();
    private final AtomicLong sizingNs = new AtomicLong();
    private final AtomicLong exactWindows = new AtomicLong();

    private final long usableBytes;

    PartitionMemoryAccount(PartitionMemoryPolicy policy) { this(policy, Integer.MAX_VALUE); }

    PartitionMemoryAccount(PartitionMemoryPolicy policy, int splits)
    {
        policy.validate();
        usableBytes = PartitionMemoryPolicy.usableBytes(policy.getWorkerBytes(), Runtime.getRuntime().maxMemory());
        int readers = Math.max(1, Math.min(Math.min(policy.getReaderThreads(), Runtime.getRuntime().availableProcessors()), splits));
        this.policy = new PartitionMemoryPolicy(policy.getWorkerBytes(), policy.getInputBytes(), policy.getOutputBytes(),
                readers, policy.getWindowRowGroups());
        long pool = Math.max(1, usableBytes * 35 / 100);
        input = new PartitionByteBudget(Math.min(policy.getInputBytes(), pool));
        output = new PartitionByteBudget(Math.min(policy.getOutputBytes(), pool));
    }

    void validatePartitioner(TypeDescription schema, int partitions)
    {
        long batch = schema.createRowBatch(WorkerCommon.rowBatchSize, TypeDescription.Mode.NONE).getMemoryUsage();
        long tails = Math.multiplyExact(Math.multiplyExact(batch + 4L * WorkerCommon.rowBatchSize, partitions), policy.getReaderThreads());
        long reserve = usableBytes * 15 / 100;
        if (tails > reserve)
        {
            throw new WorkerException("partition tail vectors exceed reserved space: " + tails + " > " + reserve +
                    "; reduce reader threads/row batch size or use a larger worker");
        }
    }

    void checkTailBytes(long bytes)
    {
        long limit = usableBytes * 15 / 100 / policy.getReaderThreads();
        if (bytes > limit) { throw new WorkerException("partition tails exceed reserved memory: " + bytes + " > " + limit); }
    }

    void window(PartitionReadWindow window, long elapsedSizingNs)
    {
        windows.incrementAndGet(); maxGroups.accumulateAndGet(window.input.getRgLength(), Math::max);
        sizingNs.addAndGet(elapsedSizingNs);
        if (window.exactSizing) { exactWindows.incrementAndGet(); }
    }

    Map<String, Long> snapshot()
    {
        Map<String, Long> result = new LinkedHashMap<>();
        result.put("inputLimitBytes", input.limit()); result.put("inputPeakBytes", input.peak());
        result.put("inputRemainingBytes", input.used()); result.put("inputWaitNs", input.waitNs());
        result.put("outputLimitBytes", output.limit()); result.put("outputPeakBytes", output.peak());
        result.put("outputRemainingBytes", output.used()); result.put("outputWaitNs", output.waitNs());
        result.put("readWindows", windows.get()); result.put("maxWindowRowGroups", maxGroups.get());
        result.put("windowSizingNs", sizingNs.get()); result.put("exactSizingWindows", exactWindows.get());
        result.put("readerThreads", (long) policy.getReaderThreads());
        result.put("backendMemoryBytes", policy.getWorkerBytes());
        result.put("usableMemoryBytes", usableBytes);
        result.put("tailLimitBytes", usableBytes * 15 / 100);
        return result;
    }
}
