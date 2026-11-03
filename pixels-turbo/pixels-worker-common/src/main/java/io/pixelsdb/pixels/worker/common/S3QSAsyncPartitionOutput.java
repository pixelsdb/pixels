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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    private final int objectLimit;
    private final int groupTargetBytes;
    private final int earlyGroupBytes;
    private final PartitionByteBudget memory;
    private final int writerThreads;
    private PartitionPipeline pipeline;
    private final AtomicInteger outstandingWrites = new AtomicInteger();
    private final FixSizedBuffers stagingBuffers;
    private final ExecutorService columnWriterService;
    private volatile boolean closed;
    private boolean finishing;

    S3QSAsyncPartitionOutput(S3QS storage, ShuffleInfo shuffle, int producerId, int count, WorkerContext context)
    {
        this(storage, shuffle, producerId, count, context, null);
    }

    S3QSAsyncPartitionOutput(S3QS storage, ShuffleInfo shuffle, int producerId, int count,
                            WorkerContext context, PartitionByteBudget memory)
    {
        this(storage, shuffle, producerId, count, context, Constants.S3QS_BUFFER_SIZE,
                producerThreads(), memory);
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
        this.writerThreads = threads;
        this.pipeline = new PartitionPipeline(Runtime.getRuntime().availableProcessors(), threads);
        this.storage = storage;
        this.shuffle = shuffle;
        this.producerId = producerId;
        this.context = context;
        this.objectLimit = chunkBytes;
        this.groupTargetBytes = chunkBytes;
        // Spread roughly two writer waves across partitions, without making tiny groups.
        // Few partitions already fill normal groups quickly; preserve those larger groups.
        this.earlyGroupBytes = (int) Math.max(1, Math.max(chunkBytes / 8L,
                Math.min(chunkBytes, 2L * threads * chunkBytes / count)));
        this.partitions = new Partition[count];
        for (int i = 0; i < count; ++i) { partitions[i] = new Partition(); }
        this.writes = new S3QSAsyncTasks(context.getCancellation(), threads);
        this.stagingBuffers = new FixSizedBuffers(chunkBytes, threads);
        writes.afterTermination(stagingBuffers::clear);
        // Separate from writers, which wait for column futures. Reuse across files,
        // but keep ownership within this invocation, including failure/cancellation.
        this.columnWriterService = Executors.newCachedThreadPool();
        writes.afterTermination(columnWriterService::shutdown);
        context.getCancellation().own(columnWriterService);
    }

    static int producerThreads()
    {
        String configured = io.pixelsdb.pixels.common.utils.ConfigFactory.Instance().getProperty("s3qs.producer.io.threads");
        int threads = configured == null ? 0 : Integer.parseInt(configured.trim());
        if (threads < 0) { throw new IllegalArgumentException("s3qs.producer.io.threads must be nonnegative"); }
        int cores = Runtime.getRuntime().availableProcessors();
        return threads == 0 ? Math.max(1, cores - cores / 3) : threads;
    }

    void readerLimit(int readers) { pipeline = new PartitionPipeline(readers, writerThreads); }
    void acquireScan() { pipeline.acquire(writes::check); }
    void releaseScan() { pipeline.release(); }
    void scanned(long bytes, long nanos) { pipeline.scanned(bytes, nanos); }
    java.util.Map<String, Long> snapshot() { return pipeline.snapshot(); }

    void check() { writes.check(); }

    /** Ownership of batch transfers here: the scanner must never modify it again. */
    void add(int partitionId, VectorizedRowBatch batch, TypeDescription schema)
    {
        writes.check();
        if (batch.isEmpty()) { return; }
        long bytes = Math.max(1L, batch.getMemoryUsage());
        if (memory != null && !memory.tryAcquire(bytes))
        {
            // Never wait for bytes while holding the grouping lock. Pending
            // buffers must become write jobs, or no writer can free memory.
            memory.acquire(bytes, () ->
            {
                writes.check();
                if (memory.used() > memory.limit() - bytes) { flushForProgress(bytes); }
            });
        }
        boolean transferred = false;
        try
        {
            synchronized (this)
            {
                // All group selection/submission shares one lock; never hold it during byte admission or IO.
                if (closed || finishing) { throw new WorkerException("S3QS partition output is closed"); }
                writes.check();
                Partition partition = partitions[partitionId];
                if (!partition.batches.isEmpty() && partition.estimatedBytes + bytes > groupTargetBytes)
                {
                    flush(partitionId, partition);
                }
                partition.schema = schema;
                partition.batches.add(batch);
                partition.estimatedBytes += bytes;
                transferred = true;
                if (partition.estimatedBytes >= groupTargetBytes) { flush(partitionId, partition); }
            }
            feedWriters(earlyGroupBytes);
        }
        finally { if (!transferred && memory != null) { memory.release(bytes); } }
    }

    /** Under byte pressure, give idle writers the largest available groups. */
    private synchronized void flushForProgress(long requiredBytes)
    {
        if (memory.used() > memory.limit() - requiredBytes) { feedWriters(1); }
    }

    /** Keep writers supplied, preferring large groups; pressure/finish may bypass the early minimum. */
    private synchronized void feedWriters(long minimumBytes)
    {
        if (closed || finishing) { return; }
        writes.check();
        while (outstandingWrites.get() < writerThreads)
        {
            int largest = -1;
            long bytes = 0, pending = 0;
            for (int i = 0; i < partitions.length; i++)
            {
                pending += partitions[i].estimatedBytes;
                if (partitions[i].estimatedBytes > bytes) { largest = i; bytes = partitions[i].estimatedBytes; }
            }
            if (largest < 0 || bytes < minimumBytes) { return; }
            // Low-output/sparse workloads benefit little from extra small files.
            // Pressure (minimum=1) must always be able to drain their buffers.
            if (minimumBytes > 1 && pending < minimumBytes * partitions.length) { return; }
            flush(largest, partitions[largest]);
        }
    }

    private synchronized void flushPending()
    {
        finishing = true;
        for (int i = 0; i < partitions.length; i++)
        {
            flush(i, partitions[i]);
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
                        objectLimit, partition.sequence, writes::check, stagingBuffers, columnWriterService).write(batches);
            }
            finally
            {
                long nanos = System.nanoTime() - start;
                pipeline.written(reservedBytes, nanos);
                context.getWorkerMetrics().addOutputCostNs(nanos);
            }
        }, () -> {
            try { if (memory != null) { memory.release(reservedBytes); } }
            finally { outstandingWrites.decrementAndGet(); }
        }, () -> feedWriters(earlyGroupBytes));
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

    private void awaitColumns()
    {
        // A completed column future may precede its executor thread's actual exit.
        boolean interrupted = Thread.interrupted();
        try
        {
            if (!columnWriterService.awaitTermination(5, TimeUnit.SECONDS))
            {
                throw new WorkerException("column encoding threads did not terminate during cleanup");
            }
        }
        catch (InterruptedException failure)
        {
            interrupted = true;
            throw new WorkerException("interrupted draining column encoding threads", failure);
        }
        finally { if (interrupted) { Thread.currentThread().interrupt(); } }
    }

    @Override
    public void close()
    {
        closed = true;
        try { writes.close(); awaitColumns(); }
        finally
        {
            synchronized (this)
            {
                for (Partition partition : partitions)
                {
                    if (memory != null) { memory.release(partition.estimatedBytes); }
                    partition.batches.clear();
                    partition.estimatedBytes = 0;
                }
            }
        }
    }
}
