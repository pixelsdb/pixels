/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.physical.FixSizedBuffers;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.storage.s3qs.S3QS;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

/** Accepts detached partition batches while scanning; publishes END only after the DATA barrier. */
final class S3QSAsyncPartitionOutput implements AutoCloseable
{
    private static final class Partition
    {
        final AtomicLong sequence = new AtomicLong();
        List<VectorizedRowBatch> batches = new ArrayList<>();
        long estimatedBytes;
        TypeDescription schema;
    }

    private final S3QS storage;
    private final ShuffleInfo shuffle;
    private final int producerId;
    private final WorkerContext context;
    private final S3QSAsyncTasks writes;
    private final Partition[] partitions;
    private final int chunkBytes;
    private final PartitionByteBudget memory;
    private final AtomicInteger outstandingWrites = new AtomicInteger();
    private final FixSizedBuffers stagingBuffers;
    private volatile boolean closed;

    S3QSAsyncPartitionOutput(S3QS storage, ShuffleInfo shuffle, int producerId, int count, WorkerContext context)
    {
        this(storage, shuffle, producerId, count, context, null);
    }

    S3QSAsyncPartitionOutput(S3QS storage, ShuffleInfo shuffle, int producerId, int count,
                            WorkerContext context, PartitionByteBudget memory)
    {
        this(storage, shuffle, producerId, count, context, Constants.S3QS_BUFFER_SIZE,
                S3QSAsyncTasks.setting("s3qs.producer.io.threads", 4), memory);
    }

    S3QSAsyncPartitionOutput(S3QS storage, ShuffleInfo shuffle, int producerId, int count,
                            WorkerContext context, int chunkBytes, int threads)
    {
        this(storage, shuffle, producerId, count, context, chunkBytes, threads, null);
    }

    S3QSAsyncPartitionOutput(S3QS storage, ShuffleInfo shuffle, int producerId, int count,
                            WorkerContext context, int chunkBytes, int threads, PartitionByteBudget memory)
    {
        if (producerId < 0) { throw new WorkerException("producerTaskId is not set for s3qs shuffle"); }
        if (chunkBytes <= 0 || chunkBytes > Constants.S3QS_BUFFER_SIZE || count <= 0)
        {
            throw new IllegalArgumentException("invalid S3QS partition/chunk size");
        }
        this.memory = memory;
        this.storage = storage;
        this.shuffle = shuffle;
        this.producerId = producerId;
        this.context = context;
        this.chunkBytes = chunkBytes;
        this.partitions = new Partition[count];
        for (int i = 0; i < count; ++i) { partitions[i] = new Partition(); }
        this.writes = new S3QSAsyncTasks(context.getCancellation(), threads);
        this.stagingBuffers = new FixSizedBuffers(chunkBytes, threads);
        writes.afterTermination(stagingBuffers::clear);
    }

    void check() { writes.check(); }

    /** Ownership of batch transfers here: the scanner must never modify it again. */
    void add(int partitionId, VectorizedRowBatch batch, TypeDescription schema)
    {
        writes.check();
        if (batch.isEmpty()) { return; }
        long bytes = Math.max(1L, batch.getMemoryUsage());
        if (memory != null && !memory.tryAcquire(bytes))
        {
            // Never wait for bytes while holding a partition lock. Pending
            // buffers must become write jobs, or no writer can free memory.
            memory.acquire(bytes, () ->
            {
                writes.check();
                if (memory.used() > memory.limit() - bytes) { flushForProgress(); }
            });
        }
        boolean transferred = false;
        try
        {
            Partition partition = partitions[partitionId];
            synchronized (partition)
            {
                // Cleanup may race a scanner that already reserved its bytes.
                // Recheck under the same lock used to drain pending buffers.
                if (closed) { throw new WorkerException("S3QS partition output is closed"); }
                writes.check();
                if (!partition.batches.isEmpty() && partition.estimatedBytes + bytes > chunkBytes)
                {
                    flush(partitionId, partition);
                }
                partition.schema = schema;
                partition.batches.add(batch);
                partition.estimatedBytes += bytes;
                transferred = true;
                if (partition.estimatedBytes >= chunkBytes) { flush(partitionId, partition); }
            }
        }
        finally { if (!transferred && memory != null) { memory.release(bytes); } }
    }

    /** Retain mergeable batches while queued/running writes can release budget. */
    private synchronized void flushForProgress()
    {
        if (outstandingWrites.get() != 0) { return; }
        int largest = -1;
        long bytes = 0;
        for (int i = 0; i < partitions.length; i++)
        {
            synchronized (partitions[i])
            {
                if (partitions[i].estimatedBytes > bytes) { largest = i; bytes = partitions[i].estimatedBytes; }
            }
        }
        if (largest >= 0)
        {
            synchronized (partitions[largest])
            {
                if (outstandingWrites.get() == 0) { flush(largest, partitions[largest]); }
            }
        }
    }

    private void flushPending()
    {
        for (int i = 0; i < partitions.length; i++)
        {
            synchronized (partitions[i]) { flush(i, partitions[i]); }
        }
    }

    private void flush(int partitionId, Partition partition)
    {
        if (partition.batches.isEmpty()) { return; }
        List<VectorizedRowBatch> batches = partition.batches;
        TypeDescription schema = partition.schema;
        long reservedBytes = partition.estimatedBytes;
        partition.batches = new ArrayList<>();
        partition.estimatedBytes = 0;
        outstandingWrites.incrementAndGet();
        writes.submit(() -> {
            long start = System.nanoTime();
            try
            {
                new S3QSPartitionWriter(storage, schema.clone(), shuffle, partitionId, producerId, context,
                        chunkBytes, partition.sequence, writes::check, stagingBuffers).write(batches);
            }
            finally { context.getWorkerMetrics().addOutputCostNs(System.nanoTime() - start); }
        }, () -> {
            try { if (memory != null) { memory.release(reservedBytes); } }
            finally { outstandingWrites.decrementAndGet(); }
        });
    }

    /** Must be called only after every scanner has finished transferring its tail batches. */
    void finish(TypeDescription schema, Set<Integer> hashValues) throws Exception
    {
        flushPending();
        writes.await();
        long start = System.nanoTime();
        try
        {
            for (int i = 0; i < partitions.length; ++i)
            {
                writes.check();
                long count = partitions[i].sequence.get();
                if (count > 0) { hashValues.add(i); }
                storage.publish(BasePartitionWorker.createS3QSProducerEndMessage(
                        shuffle, i, producerId, 0, count, schema));
            }
        }
        finally { context.getWorkerMetrics().addOutputCostNs(System.nanoTime() - start); }
    }

    void afterTermination(Runnable cleanup) { writes.afterTermination(cleanup); }

    @Override
    public void close()
    {
        closed = true;
        try { writes.close(); }
        finally
        {
            for (Partition partition : partitions)
            {
                synchronized (partition)
                {
                    if (memory != null) { memory.release(partition.estimatedBytes); }
                    partition.batches.clear();
                    partition.estimatedBytes = 0;
                }
            }
        }
    }
}
