/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.storage.s3qs.S3QS;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

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

    S3QSAsyncPartitionOutput(S3QS storage, ShuffleInfo shuffle, int producerId, int count, WorkerContext context)
    {
        this(storage, shuffle, producerId, count, context, Constants.S3QS_BUFFER_SIZE,
                S3QSAsyncTasks.setting("s3qs.producer.io.threads", 4),
                S3QSAsyncTasks.setting("s3qs.producer.io.max.inflight", 8));
    }

    S3QSAsyncPartitionOutput(S3QS storage, ShuffleInfo shuffle, int producerId, int count,
                            WorkerContext context, int chunkBytes, int threads, int maxInFlight)
    {
        if (producerId < 0) { throw new WorkerException("producerTaskId is not set for s3qs shuffle"); }
        if (chunkBytes <= 0 || chunkBytes > Constants.S3QS_BUFFER_SIZE || count <= 0)
        {
            throw new IllegalArgumentException("invalid S3QS partition/chunk size");
        }
        this.storage = storage;
        this.shuffle = shuffle;
        this.producerId = producerId;
        this.context = context;
        this.chunkBytes = chunkBytes;
        this.partitions = new Partition[count];
        for (int i = 0; i < count; ++i) { partitions[i] = new Partition(); }
        this.writes = new S3QSAsyncTasks(context.getCancellation(), threads, maxInFlight);
    }

    void check() { writes.check(); }

    /** Ownership of batch transfers here: the scanner must never modify it again. */
    void add(int partitionId, VectorizedRowBatch batch, TypeDescription schema)
    {
        writes.check();
        if (batch.isEmpty()) { return; }
        Partition partition = partitions[partitionId];
        synchronized (partition)
        {
            long bytes = Math.max(1L, batch.getMemoryUsage());
            if (!partition.batches.isEmpty() && partition.estimatedBytes + bytes > chunkBytes)
            {
                flush(partitionId, partition);
            }
            partition.schema = schema;
            partition.batches.add(batch);
            partition.estimatedBytes += bytes;
            if (partition.estimatedBytes >= chunkBytes) { flush(partitionId, partition); }
        }
    }

    private void flush(int partitionId, Partition partition)
    {
        if (partition.batches.isEmpty()) { return; }
        List<VectorizedRowBatch> batches = partition.batches;
        TypeDescription schema = partition.schema;
        partition.batches = new ArrayList<>();
        partition.estimatedBytes = 0;
        writes.submit(() -> {
            long start = System.nanoTime();
            try
            {
                new S3QSPartitionWriter(storage, schema.clone(), shuffle, partitionId, producerId, context,
                        chunkBytes, partition.sequence, writes::check).write(batches);
            }
            finally { context.getWorkerMetrics().addOutputCostNs(System.nanoTime() - start); }
        });
    }

    /** Must be called only after every scanner has finished transferring its tail batches. */
    void finish(TypeDescription schema, Set<Integer> hashValues) throws Exception
    {
        for (int i = 0; i < partitions.length; ++i) { flush(i, partitions[i]); }
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

    @Override
    public void close() { writes.close(); }
}
