/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.FixSizedBuffers;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.storage.s3qs.PhysicalS3QSWriter;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import io.pixelsdb.pixels.storage.s3qs.S3QueueMessage;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Writes independently readable, size-bounded Pixels objects for one producer/partition. */
final class S3QSPartitionWriter
{
    private final S3QS storage;
    private final TypeDescription schema;
    private final ShuffleInfo shuffle;
    private final int partitionId;
    private final int producerId;
    private final WorkerContext context;
    private final int limit;
    private final AtomicLong sequence;
    private final Runnable publishGuard;
    private final FixSizedBuffers stagingBuffers;

    S3QSPartitionWriter(S3QS storage, TypeDescription schema, ShuffleInfo shuffle,
                        int partitionId, int producerId, WorkerContext context)
    {
        this(storage, schema, shuffle, partitionId, producerId, context, Constants.S3QS_BUFFER_SIZE);
    }

    // Smaller limits allow deterministic file-tail and variable-width boundary tests.
    S3QSPartitionWriter(S3QS storage, TypeDescription schema, ShuffleInfo shuffle,
                        int partitionId, int producerId, WorkerContext context, int limit)
    {
        this(storage, schema, shuffle, partitionId, producerId, context, limit,
                new AtomicLong(), context.getCancellation()::check);
    }

    S3QSPartitionWriter(S3QS storage, TypeDescription schema, ShuffleInfo shuffle,
                        int partitionId, int producerId, WorkerContext context, int limit,
                        AtomicLong sequence, Runnable publishGuard)
    {
        this(storage, schema, shuffle, partitionId, producerId, context, limit,
                sequence, publishGuard, new FixSizedBuffers(limit, 1));
    }

    S3QSPartitionWriter(S3QS storage, TypeDescription schema, ShuffleInfo shuffle,
                        int partitionId, int producerId, WorkerContext context, int limit,
                        AtomicLong sequence, Runnable publishGuard, FixSizedBuffers stagingBuffers)
    {
        if (stagingBuffers.getBufferSize() != limit) { throw new IllegalArgumentException("staging size differs from object limit"); }
        if (limit <= 0 || limit > Constants.S3QS_BUFFER_SIZE)
        {
            throw new IllegalArgumentException("invalid S3QS object size limit: " + limit);
        }
        this.storage = storage;
        this.schema = schema;
        this.shuffle = shuffle;
        this.partitionId = partitionId;
        this.producerId = producerId;
        this.context = context;
        this.limit = limit;
        this.sequence = sequence;
        this.publishGuard = publishGuard;
        this.stagingBuffers = stagingBuffers;
    }

    /** Returns the next DATA sequence, which is also the END sequence for this partition. */
    long write(Iterable<VectorizedRowBatch> batches) throws IOException
    {
        List<Slice> chunk = new ArrayList<>();
        long estimatedBytes = 0;
        for (VectorizedRowBatch batch : batches)
        {
            context.getCancellation().check();
            if (batch.isEmpty())
            {
                continue;
            }
            // Only a grouping heuristic: the complete encoded file below determines whether it fits.
            long estimate = Math.max(1L, batch.getMemoryUsage());
            if (!chunk.isEmpty() && estimatedBytes + estimate > limit)
            {
                writeChunk(chunk);
                chunk.clear();
                estimatedBytes = 0;
            }
            chunk.add(new Slice(batch, 0, batch.size));
            estimatedBytes += estimate;
        }
        if (!chunk.isEmpty())
        {
            writeChunk(chunk);
        }
        return sequence.get();
    }

    private void writeChunk(List<Slice> slices) throws IOException
    {
        publishGuard.run();
        context.getCancellation().check();
        StagingWriter encoded = encode(slices);
        if (encoded == null)
        {
            long rows = slices.stream().mapToLong(slice -> slice.length).sum();
            if (rows == 1)
            {
                throw new IOException("single row including Pixels file metadata exceeds S3QS object limit " + limit);
            }
            List<Slice> left = new ArrayList<>(), right = new ArrayList<>();
            long remaining = rows / 2;
            for (Slice slice : slices)
            {
                int first = (int) Math.min(remaining, slice.length);
                if (first > 0)
                {
                    left.add(new Slice(slice.batch, slice.offset, first));
                    remaining -= first;
                }
                if (first < slice.length)
                {
                    right.add(new Slice(slice.batch, slice.offset + first, slice.length - first));
                }
            }
            writeChunk(left);
            writeChunk(right);
            return;
        }

        try
        {
            context.getCancellation().check();
            publishGuard.run();
            S3QueueMessage message = BasePartitionWorker.createS3QSDataMessage(
                    shuffle, partitionId, producerId, 0, sequence.getAndIncrement(), schema)
                    .setRowCount(slices.stream().mapToLong(slice -> slice.length).sum())
                    .setByteSize(encoded.position);
            PhysicalS3QSWriter physical = (PhysicalS3QSWriter) storage.offer(message);
            physical.setPublishGuard(publishGuard);
            // Do not close after a failed append: that could publish a partial object.
            physical.append(encoded.bytes, 0, (int) encoded.position);
            physical.close();
            physical.requirePublished();
            context.getWorkerMetrics().addWriteBytes(encoded.position);
            context.getWorkerMetrics().addNumWriteRequests(1);
        }
        finally { encoded.release(); }
    }

    private StagingWriter encode(List<Slice> slices) throws IOException
    {
        StagingWriter staging = new StagingWriter(limit, stagingBuffers);
        boolean retained = false;
        try
        {
            // Only local staging is closed on failure; no remote object is published here.
            try (PixelsWriter writer = WorkerCommon.getWriter(schema, storage, staging.getPath(), staging, false, null))
            {
                for (Slice slice : slices)
                {
                    context.getCancellation().check();
                    if (slice.offset == 0 && slice.length == slice.batch.size)
                    {
                        writer.addRowBatch(slice.batch);
                    }
                    else
                    {
                        VectorizedRowBatch selected = schema.createRowBatch(slice.length, TypeDescription.Mode.NONE);
                        int[] indexes = new int[slice.length];
                        for (int i = 0; i < indexes.length; i++) { indexes[i] = slice.offset + i; }
                        selected.addSelected(indexes, 0, indexes.length, slice.batch);
                        writer.addRowBatch(selected);
                    }
                }
            }
            if (!staging.closed) { throw new IOException("Pixels writer did not finish encoding S3QS object"); }
            retained = !staging.overflow;
            return retained ? staging : null;
        }
        finally { if (!retained) { staging.release(); } }
    }

    private static final class Slice
    {
        final VectorizedRowBatch batch;
        final int offset, length;

        Slice(VectorizedRowBatch batch, int offset, int length)
        {
            this.batch = batch;
            this.offset = offset;
            this.length = length;
        }
    }

    /** Counts the complete file (including its tail), retaining at most limit bytes. */
    private static final class StagingWriter implements PhysicalWriter
    {
        private final int limit;
        private final byte[] bytes;
        private final FixSizedBuffers buffers;
        private boolean overflow;
        private boolean released;
        private long position;
        private boolean closed;

        StagingWriter(int limit, FixSizedBuffers buffers)
        {
            this.limit = limit;
            this.buffers = buffers;
            this.bytes = buffers.allocate();
        }

        void release()
        {
            if (!released)
            {
                released = true;
                buffers.free(bytes);
            }
        }

        @Override public long prepare(int length) { return position; }
        @Override public long append(ByteBuffer buffer)
        {
            ((Buffer) buffer).flip();
            return append(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }
        @Override public long append(byte[] buffer, int offset, int length)
        {
            long start = position;
            position += length;
            if (position > limit)
            {
                overflow = true; // Keep counting, then reuse the array for smaller trials.
            }
            else if (!overflow)
            {
                System.arraycopy(buffer, offset, bytes, (int) start, length);
            }
            return start;
        }
        @Override public void close() { closed = true; }
        @Override public void flush() { }
        @Override public String getPath() { return "s3qs-staging"; }
        @Override public int getBufferSize() { return limit; }
    }
}
