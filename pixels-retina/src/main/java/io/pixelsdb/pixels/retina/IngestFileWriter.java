/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 */
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.PixelsFileNameUtils;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.ingest.IngestColumnBatch;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.ingest.IngestProto.BufferSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A bounded, append-only Pixels file builder for FILE ingestion.
 *
 * <p>This path deliberately does not use {@link PixelsWriteBuffer}, {@link MemTable}, or the
 * Retina object-staging directory. Committed WAL batches are decoded into bounded row batches and
 * appended directly to a temporary Pixels file. The file becomes catalog-visible only after its
 * physical footer and MainIndex entries are durable.</p>
 */
public final class IngestFileWriter implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(IngestFileWriter.class);

    private final long tableId;
    private final TypeDescription schema;
    private final int[] orderMapping;
    private final int[] outputColumns;
    private final Path targetPath;
    private final Storage storage;
    private final String hostName;
    private final int virtualNodeId;
    private final int fileTargetRows;
    private final int pixelStride;
    private final long blockSize;
    private final short replication;
    private final EncodingLevel encodingLevel;
    private final boolean nullsPadding;
    private final MetadataService metadata;
    private final IndexService indexes;
    private final RetinaResourceManager resources;

    private ActiveFile active;
    private boolean failed;

    private static final class ActiveFile
    {
        private final File file;
        private final PixelsWriter writer;
        private final VectorizedRowBatch batch;
        private final int capacity;
        private int rows;
        private long minRowId = Long.MAX_VALUE;
        private long maxRowId = Long.MIN_VALUE;
        private long minCommitTimestamp = Long.MAX_VALUE;
        private boolean physicalClosed;
        private boolean indexFlushed;

        private ActiveFile(File file, PixelsWriter writer, int capacity, VectorizedRowBatch batch)
        {
            this.file = file;
            this.writer = writer;
            this.capacity = capacity;
            this.batch = batch;
        }
    }

    public IngestFileWriter(
            long tableId,
            TypeDescription schema,
            int[] orderMapping,
            Path targetPath,
            String hostName,
            int virtualNodeId,
            int fileTargetRows,
            int pixelStride,
            MetadataService metadata,
            IndexService indexes,
            RetinaResourceManager resources) throws RetinaException
    {
        checkArgument(fileTargetRows > 0, "fileTargetRows must be positive");
        checkArgument(pixelStride > 0, "pixelStride must be positive");
        this.tableId = tableId;
        this.schema = schema;
        this.orderMapping = orderMapping;
        this.outputColumns = new int[orderMapping.length];
        boolean[] mapped = new boolean[orderMapping.length];
        for (int output = 0; output < orderMapping.length; output++)
        {
            int source = orderMapping[output];
            checkArgument(source >= 0 && source < orderMapping.length && !mapped[source],
                    "Physical column order must be a permutation");
            outputColumns[source] = output;
            mapped[source] = true;
        }
        this.targetPath = targetPath;
        this.hostName = hostName;
        this.virtualNodeId = virtualNodeId;
        this.fileTargetRows = fileTargetRows;
        this.pixelStride = pixelStride;
        this.metadata = metadata;
        this.indexes = indexes;
        this.resources = resources;

        ConfigFactory config = ConfigFactory.Instance();
        this.blockSize = Long.parseLong(config.getProperty("block.size"));
        this.replication = Short.parseShort(config.getProperty("block.replication"));
        this.encodingLevel = EncodingLevel.from(Integer.parseInt(
                config.getProperty("retina.buffer.flush.encodingLevel")));
        this.nullsPadding = Boolean.parseBoolean(
                config.getProperty("retina.buffer.flush.nullsPadding"));
        try
        {
            this.storage = StorageFactory.Instance().getStorage(targetPath.getUri());
        }
        catch (IOException e)
        {
            throw new RetinaException("Failed to open ingest file storage", e);
        }
    }

    /** Reserves the next physical range; the caller persists it before calling append. */
    public synchronized BufferSpan planSpan(int remaining, long rowIdStart) throws RetinaException
    {
        ensureUsable();
        if (remaining <= 0)
        {
            throw new RetinaException("Cannot plan an empty file span");
        }
        if (active == null)
        {
            active = create(null);
        }
        if (active.physicalClosed || active.rows >= active.capacity)
        {
            throw new RetinaException("Previous ingest file has not been published");
        }
        int count = Math.min(remaining, active.capacity - active.rows);
        return BufferSpan.newBuilder()
                .setFileId(active.file.getId())
                .setFileName(active.file.getName())
                .setPathId(active.file.getPathId())
                .setFileCapacity(active.capacity)
                .setBlockStartOffset(active.rows)
                .setRowCount(count)
                .setRowIdStart(rowIdStart)
                .build();
    }

    /** Appends a persisted span directly to PixelsWriter using bounded row batches. */
    public synchronized void append(
            BufferSpan span, List<byte[][]> rows, long commitTimestamp) throws RetinaException
    {
        append(span, rows, null, commitTimestamp);
    }

    public synchronized void append(
            BufferSpan span, IngestColumnBatch columns, long commitTimestamp) throws RetinaException
    {
        append(span, null, columns, commitTimestamp);
    }

    private void append(BufferSpan span, List<byte[][]> rows, IngestColumnBatch columns,
                        long commitTimestamp) throws RetinaException
    {
        ensureUsable();
        if ((columns == null ? span.getRowCount() != rows.size()
                : span.getRowCount() > columns.remaining()) || span.getFileCapacity() <= 0)
        {
            throw new RetinaException("Invalid direct-file installation span");
        }
        if (active == null)
        {
            active = create(span);
        }
        validateIdentity(span);
        int start = span.getBlockStartOffset();
        int expectedEnd;
        try
        {
            expectedEnd = Math.addExact(start, span.getRowCount());
        }
        catch (ArithmeticException e)
        {
            throw new RetinaException("Direct-file installation span overflows", e);
        }
        if (start < 0 || expectedEnd > active.capacity)
        {
            throw new RetinaException("Direct-file installation span is out of bounds");
        }
        if (expectedEnd <= active.rows)
        {
            if (columns != null)
            {
                try
                {
                    columns.skip(span.getRowCount());
                }
                catch (IOException e)
                {
                    throw new RetinaException("Invalid replay window", e);
                }
            }
            return;
        }
        if (active.rows != start)
        {
            throw new RetinaException("Direct-file installation is not contiguous");
        }

        try
        {
            int offset = 0;
            while (offset < span.getRowCount())
            {
                int count = Math.min(pixelStride, span.getRowCount() - offset);
                VectorizedRowBatch batch = active.batch;
                try
                {
                    if (columns != null)
                    {
                        columns.appendWindow(batch, outputColumns, count, commitTimestamp);
                    }
                    else
                    {
                        for (int row = 0; row < count; row++)
                        {
                            byte[][] values = rows.get(offset + row);
                            for (int column = 0; column < orderMapping.length; column++)
                            {
                                byte[] value = values[orderMapping[column]];
                                if (value == null)
                                {
                                    batch.cols[column].addNull();
                                }
                                else
                                {
                                    batch.cols[column].add(value);
                                }
                            }
                            batch.cols[schema.getChildren().size()].add(commitTimestamp);
                            batch.size++;
                        }
                    }
                    active.writer.addRowBatch(batch);
                }
                finally
                {
                    // PixelsWriter consumes the batch synchronously, as in the CLI loader.
                    // Reset releases variable-width row references before the next contribution.
                    batch.reset();
                }
                offset += count;
            }
            active.rows = expectedEnd;
            active.minRowId = Math.min(active.minRowId, span.getRowIdStart());
            active.maxRowId = Math.max(
                    active.maxRowId, span.getRowIdStart() + span.getRowCount() - 1L);
            active.minCommitTimestamp = Math.min(active.minCommitTimestamp, commitTimestamp);
        }
        catch (Exception e)
        {
            failed = true;
            throw new RetinaException("Failed to append directly to ingest Pixels file", e);
        }
    }

    /** Publishes a full file after the caller has installed every index entry for the span. */
    public synchronized void publishIfFull() throws RetinaException
    {
        if (active != null && active.rows == active.capacity)
        {
            publishActive();
        }
    }

    /** Closes and publishes a non-empty tail for delay, pressure, shutdown, or a barrier. */
    public synchronized boolean publishTail() throws RetinaException
    {
        if (active == null || active.rows == 0)
        {
            return false;
        }
        publishActive();
        return true;
    }

    public synchronized long getEarliestPendingMinTs()
    {
        return active == null ? Long.MAX_VALUE : active.minCommitTimestamp;
    }

    public synchronized long getActiveFileId()
    {
        return active == null ? 0L : active.file.getId();
    }

    public synchronized int getActiveRowCount()
    {
        return active == null ? 0 : active.rows;
    }

    public int getVirtualNodeId()
    {
        return virtualNodeId;
    }

    private ActiveFile create(BufferSpan restored) throws RetinaException
    {
        String fileName = restored == null
                ? PixelsFileNameUtils.buildOrderedFileName(hostName, virtualNodeId)
                : restored.getFileName();
        String filePath = targetPath.getUri() + "/" + fileName;
        File file = null;
        PixelsWriter writer = null;
        boolean registered = false;
        boolean visibilityAdded = false;
        try
        {
            if (restored == null)
            {
                file = new File();
                file.setName(fileName);
                file.setType(File.Type.TEMPORARY_INGEST);
                file.setNumRowGroup(1);
                file.setPathId(targetPath.getId());
                if (!metadata.addFiles(Collections.singletonList(file)))
                {
                    throw new MetadataException("Failed to register direct ingest file");
                }
                registered = true;
                file.setId(metadata.getFileId(filePath));
            }
            else
            {
                file = metadata.getFileById(restored.getFileId());
                if (file == null
                        || file.getType() != File.Type.TEMPORARY_INGEST
                        || file.getPathId() != restored.getPathId()
                        || !file.getName().equals(fileName)
                        || restored.getBlockStartOffset() != 0)
                {
                    throw new MetadataException("Recorded direct ingest file cannot be restored");
                }
            }
            int capacity = restored == null ? fileTargetRows : restored.getFileCapacity();
            resources.addVisibility(file.getId(), 0, capacity, 0L, null, false);
            visibilityAdded = true;
            writer = PixelsWriterImpl.newBuilder()
                    .setSchema(schema)
                    .setHasHiddenColumn(true)
                    .setPixelStride(pixelStride)
                    .setRowGroupSize(Integer.MAX_VALUE)
                    .setStorage(storage)
                    .setPath(filePath)
                    .setOverwrite(restored != null)
                    .setBlockSize(blockSize)
                    .setReplication(replication)
                    .setBlockPadding(true)
                    .setEncodingLevel(encodingLevel)
                    .setNullsPadding(nullsPadding)
                    .setCompressionBlockSize(1)
                    .build();
            return new ActiveFile(file, writer, capacity,
                    schema.createRowBatchWithHiddenColumn(Math.min(pixelStride, capacity)));
        }
        catch (Exception e)
        {
            if (restored == null)
            {
                cleanupFailedCreation(filePath, file, writer, registered, visibilityAdded);
            }
            throw new RetinaException("Failed to create direct ingest Pixels file " + filePath, e);
        }
    }

    private void cleanupFailedCreation(
            String filePath,
            File file,
            PixelsWriter writer,
            boolean registered,
            boolean visibilityAdded)
    {
        if (writer != null)
        {
            try
            {
                writer.abort();
            }
            catch (Exception cleanupFailure)
            {
                logger.warn("Failed to abort direct ingest file writer for {}", filePath,
                        cleanupFailure);
            }
        }
        if (visibilityAdded && file != null)
        {
            resources.removeVisibility(file.getId());
        }
        try
        {
            if (storage.exists(filePath))
            {
                storage.delete(filePath, false);
            }
        }
        catch (Exception cleanupFailure)
        {
            logger.warn("Failed to delete partial direct ingest file {}", filePath,
                    cleanupFailure);
        }
        if (registered && file != null && file.getId() > 0)
        {
            try
            {
                if (!metadata.deleteFiles(Collections.singletonList(file.getId())))
                {
                    logger.warn("Failed to delete metadata for direct ingest file {}", filePath);
                }
            }
            catch (Exception cleanupFailure)
            {
                logger.warn("Failed to delete metadata for direct ingest file {}", filePath,
                        cleanupFailure);
            }
        }
    }

    private void validateIdentity(BufferSpan span) throws RetinaException
    {
        if (active.file.getId() != span.getFileId()
                || active.file.getPathId() != span.getPathId()
                || !active.file.getName().equals(span.getFileName())
                || active.capacity != span.getFileCapacity())
        {
            throw new RetinaException("Recorded direct-file identity changed");
        }
    }

    private void publishActive() throws RetinaException
    {
        ensureUsable();
        if (active == null || active.rows == 0)
        {
            return;
        }
        try
        {
            if (!active.physicalClosed)
            {
                active.writer.close();
                active.batch.close();
                active.physicalClosed = true;
            }
            if (!active.indexFlushed)
            {
                if (!indexes.flushMainIndexOfFile(tableId, active.file.getId()))
                {
                    throw new RetinaException(
                            "Failed to flush MainIndex for direct ingest file " + active.file.getId());
                }
                active.indexFlushed = true;
            }
            File regular = new File();
            regular.setId(active.file.getId());
            regular.setName(active.file.getName());
            regular.setType(File.Type.REGULAR);
            regular.setNumRowGroup(1);
            regular.setPathId(active.file.getPathId());
            regular.setMinRowId(active.minRowId);
            regular.setMaxRowId(active.maxRowId);
            if (!metadata.updateFile(regular))
            {
                throw new RetinaException(
                        "Failed to publish direct ingest file " + active.file.getId());
            }
            active = null;
        }
        catch (IndexException | MetadataException e)
        {
            throw new RetinaException("Failed to publish direct ingest file", e);
        }
        catch (IOException e)
        {
            failed = true;
            throw new RetinaException("Failed to close direct ingest file", e);
        }
    }

    private void ensureUsable() throws RetinaException
    {
        if (failed)
        {
            throw new RetinaException("Direct ingest file writer is failed; recovery is required");
        }
    }

    @Override
    public synchronized void close() throws RetinaException
    {
        publishTail();
    }
}
