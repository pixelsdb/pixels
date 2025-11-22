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
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Responsible for managing several blocks of data and writing them to a file.
 */
public class FileWriterManager
{
    private final long tableId;
    private final PixelsWriter writer;
    private final File file;

    // [firstBlockId, lastBlockId]
    private final long firstBlockId;
    private long lastBlockId = -1;

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
                             long firstBlockId, int recordNum) throws RetinaException
    {
        this.tableId = tableId;
        this.firstBlockId = firstBlockId;

        // Create pixels writer.
        String targetFileName = DateUtil.getCurTime() + ".pxl";
        String targetFilePath = targetOrderedDirPath.getUri() + "/" + targetFileName;
        try
        {
            // Add file information to the metadata.
            MetadataService metadataService = MetadataService.Instance();
            file = new File();
            this.file.setName(targetFileName);
            this.file.setType(File.Type.TEMPORARY);
            this.file.setNumRowGroup(1);
            this.file.setPathId(targetOrderedDirPath.getId());
            metadataService.addFiles(Collections.singletonList(file));
            this.file.setId(metadataService.getFileId(targetFilePath));
        } catch (MetadataException e)
        {
            throw new RetinaException("Failed to add file information to the metadata", e);
        }

        // Add the corresponding visibility for the file.
        RetinaResourceManager retinaResourceManager = RetinaResourceManager.Instance();
        retinaResourceManager.addVisibility(this.file.getId(), 0, recordNum);

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
            throw new RetinaException("Failed to create pixels writer", e);
        }
    }

    public long getFileId()
    {
        return this.file.getId();
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

    public void addRowBatch(VectorizedRowBatch rowBatch) throws RetinaException
    {
        try
        {
            this.writer.addRowBatch(rowBatch);
        } catch (IOException e)
        {
            throw new RetinaException("Failed to add rowBatch to pixels writer", e);
        }
    }

    /**
     * Create a background thread to write the block of data stored in shared storage to a file.
     */
    public CompletableFuture<Void> finish()
    {
        CompletableFuture<Void> future = new CompletableFuture<>();

        new Thread(() -> {
            try {
                for (long blockId = firstBlockId; blockId <= lastBlockId; ++blockId)
                {
                    ObjectStorageManager objectStorageManager = ObjectStorageManager.Instance();
                    /*
                     * Issue-1083: Since we obtain a read-only ByteBuffer from the S3 Reader,
                     * we cannot read a byte[]. Instead, we should return the ByteBuffer directly.
                     */
                    ByteBuffer data = objectStorageManager.read(this.tableId, blockId);
                    this.writer.addRowBatch(VectorizedRowBatch.deserialize(data));
                }
                this.writer.close();

                // Update the file's type.
                this.file.setType(File.Type.REGULAR);
                MetadataService metadataService = MetadataService.Instance();
                metadataService.updateFile(this.file);

                future.complete(null);
            } catch (Exception e)
            {
                future.completeExceptionally(e);
            }
        }).start();

        return future;
    }
}
