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

    private final long tableId;
    private final PixelsWriter writer;
    private final File file;

    // [firstBlockId, lastBlockId]
    private final long firstBlockId;
    private long lastBlockId;

    /**
     * Creating pixelsWriter by passing in parameters avoids the need to read
     * the configuration file for each call.
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

        // create pixels writer
        String targetFileName = DateUtil.getCurTime() + ".pxl";
        String targetFilePath = targetOrderedDirPath.getUri() + "/" + targetFileName;
        try
        {
            // add file information to the metadata
            MetadataService metadataService = MetadataService.Instance();
            file = new File();
            this.file.setName(targetFileName);
            this.file.setType(File.Type.EMPTY);
            this.file.setNumRowGroup(1);
            this.file.setPathId(targetOrderedDirPath.getId());
            metadataService.addFiles(Collections.singletonList(file));
            this.file.setId(metadataService.getFileId(targetFilePath));
        } catch (MetadataException e)
        {
            logger.error("Failed to add file into metadata", e);
            throw new RetinaException("Failed to add file into metadata", e);
        }

        // add the file's visibility
        RetinaResourceManager retinaResourceManager = RetinaResourceManager.Instance();
        retinaResourceManager.addVisibility(this.file.getId(), 0, recordNum);

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
                    MinioManager minioManager = MinioManager.Instance();
                    byte[] data = minioManager.read(this.tableId, blockId);
                    this.writer.addRowBatch(VectorizedRowBatch.deserialize(data));
                }
                this.writer.close();

                // update file's type
                this.file.setType(File.Type.REGULAR);
                MetadataService metadataService = MetadataService.Instance();
                metadataService.updateFile(this.file);
            } catch (Exception e)
            {
                logger.error("Failed to flush to disk file", e);
            }
        }).start();
    }
}
