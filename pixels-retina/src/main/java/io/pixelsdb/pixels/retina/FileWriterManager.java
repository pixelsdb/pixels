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
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;

/**
 * Responsible for several blocks of data and written to a file
 */
public class FileWriterManager
{
    private static final Logger logger = LogManager.getLogger(FileWriterManager.class);

    private final Storage minio;
    private final String schemaName;
    private final String tableName;
    private final PixelsWriter writer;
    private final long fileId;

    // [firstBlockId, lastBlockId]
    private final long firstBlockId;
    private long lastBlockId;

    /**
     * Creating pixelsWriter by passing in parameters avoids the need to read
     * the configuration file for each call.
     * @param schemaName
     * @param tableName
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
    public FileWriterManager(Storage minio, String schemaName, String tableName,
                             TypeDescription schema, Path targetOrderedDirPath,
                             Storage targetOrderedStorage, int pixelsStride,
                             long blockSize, short replication, EncodingLevel encodingLevel,
                             boolean nullsPadding, long firstBlockId) throws RetinaException
    {
        this.minio = minio;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.firstBlockId = firstBlockId;

        // create pixels writer
        String targetFileName = DateUtil.getCurTime() + ".pxl";
        String targetFilePath = targetOrderedDirPath.getUri() + targetFileName;
        try
        {
            // add file information to the metadata
            MetadataService metadataService = MetadataService.Instance();
            File addedFile = new File();
            addedFile.setName(targetFileName);
            addedFile.setNumRowGroup(1);
            addedFile.setPathId(targetOrderedDirPath.getId());
            metadataService.addFiles(Collections.singletonList(addedFile));
            // this.fileId = metadataService.getFileId(targetFilePath);
            this.fileId = 0;
        } catch (MetadataException e)
        {
            logger.error("Failed to add file into metadata", e);
            throw new RetinaException("Failed to add file into metadata", e);
        }

        try
        {
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
            logger.error("Failed to create pixels writer", e);
            throw new RetinaException("Failed to create pixels writer", e);
        }
    }

    public long getFileId()
    {
        return this.fileId;
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

    public void addRowBatch(VectorizedRowBatch rowBatch) throws IOException
    {
        this.writer.addRowBatch(rowBatch);
    }

    /**
     * Create a background thread to write the block of data responsible in minio to a file
     */
    public void finish()
    {
        new Thread(() -> {
            try
            {
                for (long blockId = firstBlockId; blockId <= lastBlockId; ++blockId)
                {
                    String minioEntry = this.schemaName + '/' + this.tableName + '/' + blockId;
                    PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(this.minio, minioEntry);
                    int length = (int) reader.getFileLength();
                    byte[] data = new byte[length];
                    reader.readFully(data, 0, length);
                    this.writer.addRowBatch(VectorizedRowBatch.deserialize(data));
                }
                this.writer.close();
            } catch (Exception e)
            {
                logger.error("Failed to flush to disk file", e);
            }
        }).start();
    }
}
