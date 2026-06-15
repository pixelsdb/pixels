/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.PixelsFileNameUtils;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collections;

/**
 * Responsible for managing several blocks of data and writing them to a file.
 */
public class FileWriterManager
{
    private static final Logger logger = LogManager.getLogger(FileWriterManager.class);

    private final long tableId;
    private final PixelsWriter writer;
    private final File file;

    // [firstBlockId, lastBlockId]
    private final long firstBlockId;
    private long lastBlockId = -1;
    private final int virtualNodeId;

    // [fileMinRowId, fileMaxRowId] is the range of row ids in the file.
    private long fileMinRowId = Long.MAX_VALUE;
    private long fileMaxRowId = Long.MIN_VALUE;

    private volatile boolean physicalClosed;
    private volatile RetinaException physicalCloseFailure;

    // Signals that the index has been flushed.
    private volatile boolean indexFlushed;

    /**
     * Creating pixelsWriter by passing in parameters avoids the need to read
     * the configuration file for each call.
     *
     * @param tableId
     * @param schema
     * @param targetOrderedDirPath
     * @param pixelsStride
     * @param targetOrderedStorage
     * @param blockSize
     * @param replication
     * @param encodingLevel
     * @param nullsPadding
     * @throws RetinaException
     */
    public FileWriterManager(long tableId, TypeDescription schema,
                             Path targetOrderedDirPath, Storage targetOrderedStorage,
                             int pixelsStride, long blockSize, short replication,
                             EncodingLevel encodingLevel, boolean nullsPadding,
                             long firstBlockId, int recordNum, String hostName, int virtualNodeId) throws RetinaException
    {
        this.tableId = tableId;
        this.firstBlockId = firstBlockId;
        this.virtualNodeId = virtualNodeId;

        // Create pixels writer.
        String targetFileName = PixelsFileNameUtils.buildOrderedFileName(hostName, virtualNodeId);
        String targetFilePath = targetOrderedDirPath.getUri() + "/" + targetFileName;
        try
        {
            // Add file information to the metadata.
            MetadataService metadataService = MetadataService.Instance();
            file = new File();
            this.file.setName(targetFileName);
            this.file.setType(File.Type.TEMPORARY_INGEST);
            this.file.setNumRowGroup(1);
            this.file.setPathId(targetOrderedDirPath.getId());
            if (!metadataService.addFiles(Collections.singletonList(file)))
            {
                throw new MetadataException("failed to add metadata for ingest file " + targetFilePath);
            }
            this.file.setId(metadataService.getFileId(targetFilePath));
        } catch (MetadataException e)
        {
            throw new RetinaException("Failed to add file information to the metadata, " +
                    "targetFilePath: " + targetFilePath, e);
        }

        // Add the corresponding visibility for the file.
        RetinaResourceManager retinaResourceManager = RetinaResourceManager.Instance();
        retinaResourceManager.addVisibility(this.file.getId(), 0, recordNum, 0L, null, false);

        try
        {
            // Create file writer.
            this.writer = PixelsWriterImpl.newBuilder()
                    .setSchema(schema)
                    .setHasHiddenColumn(true)
                    .setPixelStride(pixelsStride)
                    .setRowGroupSize(Integer.MAX_VALUE)
                    .setStorage(targetOrderedStorage)
                    .setPath(targetFilePath)
                    .setBlockSize(blockSize)
                    .setReplication(replication)
                    .setBlockPadding(true)
                    .setEncodingLevel(encodingLevel)
                    .setNullsPadding(nullsPadding)
                    .setCompressionBlockSize(1)
                    .build();
        } catch (Exception e)
        {
            retinaResourceManager.removeVisibility(this.file.getId());
            try
            {
                if (!MetadataService.Instance().deleteFiles(Collections.singletonList(this.file.getId())))
                {
                    logger.warn("Failed to delete metadata for ingest file after writer creation failure, fileId={}",
                            this.file.getId());
                }
            }
            catch (MetadataException metadataException)
            {
                logger.warn("Failed to delete metadata for ingest file after writer creation failure, fileId={}",
                        this.file.getId(), metadataException);
            }
            throw new RetinaException("Failed to create pixels writer", e);
        }
    }

    public long getFileId()
    {
        return this.file.getId();
    }

    public String getFileName()
    {
        return this.file.getName();
    }

    public void setLastBlockId(long lastBlockId)
    {
        this.lastBlockId = lastBlockId;
    }

    public long getFirstBlockId()
    {
        return this.firstBlockId;
    }

    public long getLastBlockId()
    {
        return this.lastBlockId;
    }

    public int getVirtualNodeId()
    {
        return this.virtualNodeId;
    }

    public synchronized void includeRowId(long rowId)
    {
        this.fileMinRowId = Math.min(this.fileMinRowId, rowId);
        this.fileMaxRowId = Math.max(this.fileMaxRowId, rowId);
    }

    public synchronized boolean hasRowIds()
    {
        return this.fileMinRowId != Long.MAX_VALUE && this.fileMaxRowId != Long.MIN_VALUE;
    }

    public boolean isPhysicalClosed()
    {
        return this.physicalClosed;
    }

    public boolean isIndexFlushed()
    {
        return this.indexFlushed;
    }

    void markIndexFlushed()
    {
        this.indexFlushed = true;
    }

    public synchronized File getFileSnapshot() throws RetinaException
    {
        if (!hasRowIds())
        {
            throw new RetinaException("Cannot create file snapshot without row-id hull: fileId=" + getFileId());
        }
        File snapshot = new File();
        snapshot.setId(this.file.getId());
        snapshot.setName(this.file.getName());
        snapshot.setType(this.file.getType());
        snapshot.setNumRowGroup(this.file.getNumRowGroup());
        snapshot.setMinRowId(this.fileMinRowId);
        snapshot.setMaxRowId(this.fileMaxRowId);
        snapshot.setPathId(this.file.getPathId());
        return snapshot;
    }

    /**
     * Replay object blocks and physically close the writer.
     * Idempotent after success; failed closes rethrow the cached failure.
     */
    public synchronized void finish() throws RetinaException
    {
        if (this.physicalCloseFailure != null)
        {
            throw this.physicalCloseFailure;
        }
        if (this.physicalClosed)
        {
            return;
        }

        try
        {
            if (this.lastBlockId >= this.firstBlockId)
            {
                ObjectStorageManager objectStorageManager = ObjectStorageManager.Instance();
                for (long blockId = firstBlockId; blockId <= lastBlockId; ++blockId)
                {
                    /*
                     * Issue-1083: Since we obtain a read-only ByteBuffer from the S3 Reader,
                     * we cannot read a byte[]. Instead, we should return the ByteBuffer directly.
                     */
                    ByteBuffer data = objectStorageManager.read(this.tableId, virtualNodeId, blockId);
                    this.writer.addRowBatch(VectorizedRowBatch.deserialize(data));
                }
            }
            this.writer.close();
            this.physicalClosed = true;
        } catch (Exception e)
        {
            RetinaException wrapped = new RetinaException(
                    "Failed to physically close ingest file " + this.file.getId(), e);
            this.physicalCloseFailure = wrapped;
            throw wrapped;
        }
    }

    /**
     * Discard a zero-data ingest file by aborting the writer and removing metadata.
     * The caller deletes any half-written physical bytes before calling this.
     * Must not be called after {@link #finish()}.
     */
    public synchronized void discard() throws RetinaException
    {
        if (isPhysicalClosed())
        {
            throw new RetinaException(
                    "Cannot discard a physically closed FileWriterManager, fileId=" + getFileId());
        }
        try
        {
            this.writer.abort();
        }
        catch (Exception e)
        {
            logger.warn("FileWriterManager.discard: writer abort failed, fileId={}", getFileId(), e);
        }
        try
        {
            MetadataService.Instance().deleteFiles(Collections.singletonList(this.file.getId()));
        }
        catch (MetadataException e)
        {
            throw new RetinaException(
                    "Failed to delete TEMPORARY_INGEST file metadata, fileId=" + getFileId(), e);
        }
        RetinaResourceManager.Instance().removeVisibility(this.file.getId());
    }
}
