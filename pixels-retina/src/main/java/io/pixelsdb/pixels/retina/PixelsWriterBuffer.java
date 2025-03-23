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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

public class PixelsWriterBuffer
{
    private static final Logger logger = LogManager.getLogger(PixelsWriterBuffer.class);

    // Column information is recorded to create rowBatch.
    private final TypeDescription schema;

    // PixelsWriter and its configuration information
    private PixelsWriter writer;
    private final int pixelStride;
    private final int rowGroupSize;
    private final long blockSize;
    private final short replication;
    private final EncodingLevel encodingLevel;
    private final boolean nullsPadding;
    private final int maxBufferSize;
    private final String targetOrderedDirPath;
    private final String targetCompactDirPath;
    private final Storage targetOrderedStorage;
    private final Storage targetCompactStorage;

    // double writer buffer
    private VectorizedRowBatch activeBuffer;
    private VectorizedRowBatch immutableBuffer = null;
    private int activeBufferRowCount = 0;
    private long totalRowsWritten = 0;

    // Backend Flush
    private final ExecutorService flushExecutor;
    private final Object bufferLock = new Object();

    public PixelsWriterBuffer(TypeDescription schema, String targetOrderedDirPath, String targetCompactDirPath) throws RetinaException
    {
        this.schema = schema;

        ConfigFactory configFactory = ConfigFactory.Instance();
        this.pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
        this.rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size"));
        this.targetOrderedDirPath = targetOrderedDirPath;
        this.targetCompactDirPath = targetCompactDirPath;
        try
        {
            this.targetOrderedStorage = StorageFactory.Instance().getStorage(targetOrderedDirPath);
            this.targetCompactStorage = StorageFactory.Instance().getStorage(targetCompactDirPath);
        } catch (Exception e)
        {
            throw new RetinaException("Failed to get storage" + e);
        }
        this.blockSize = Long.parseLong(configFactory.getProperty("block.size"));
        this.replication = Short.parseShort(configFactory.getProperty("block.replication"));
        this.encodingLevel = EncodingLevel.from(Integer.parseInt(configFactory.getProperty("retina.buffer.flush.encodingLevel")));
        this.nullsPadding = Boolean.parseBoolean(configFactory.getProperty("retina.buffer.flush.nullsPadding"));
        this.maxBufferSize = Integer.parseInt(configFactory.getProperty("retina.buffer.flush.size"));
        this.activeBuffer = this.schema.createRowBatchWithHiddenColumn(this.pixelStride, TypeDescription.Mode.NONE);
        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "pixelsWriterBuffer");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * values is all column values, add values and timestamp into buffer
     * @param values
     * @param timestamp
     * @return
     */
    public boolean addRow(byte[][] values, long timestamp)
    {
        int columnCount = schema.getChildren().size();
        checkArgument(values.length == columnCount,
                "Column values count does not match schema column count");
        synchronized (bufferLock)
        {
            if (writer == null)
            {
                try
                {
                    createNewWriter();
                } catch (RetinaException e)
                {
                    throw new RuntimeException("Failed to create new writer", e);
                }
            }

            // active buffer is full
            if (activeBufferRowCount >= pixelStride)
            {
                while (immutableBuffer != null)
                {
                    try
                    {
                        bufferLock.wait();
                    } catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }

                immutableBuffer = activeBuffer;
                activeBuffer = schema.createRowBatchWithHiddenColumn(pixelStride, TypeDescription.Mode.NONE);
                activeBufferRowCount = 0;
                flushExecutor.submit(() -> {
                   try
                   {
                       flushBuffer(immutableBuffer);
                   } catch (IOException e)
                   {
                       logger.error("Failed to flush buffer", e);
                   }
                });
            }

            activeBufferRowCount++;
            activeBuffer.size++;
            for (int i = 0; i < values.length; ++i)
            {
                this.activeBuffer.cols[i].add(new String(values[i]));
            }
            this.activeBuffer.cols[columnCount].add(timestamp);
        }
        return true;
    }

    private void createNewWriter() throws RetinaException
    {
        if (writer != null)
        {
            throw new RetinaException("Writer already exists");
        }
        try
        {
            String targetFileName = DateUtil.getCurTime() + ".pxl";
            String targetFilePath = targetOrderedDirPath + targetFileName;
            writer = PixelsWriterImpl.newBuilder()
                    .setSchema(this.schema)
                    .setHasHiddenColumn(true)
                    .setPixelStride(this.pixelStride)
                    .setRowGroupSize(this.rowGroupSize)
                    .setStorage(this.targetOrderedStorage)
                    .setPath(targetFilePath)
                    .setBlockSize(this.blockSize)
                    .setReplication(this.replication)
                    .setBlockPadding(true)
                    .setEncodingLevel(this.encodingLevel)
                    .setNullsPadding(this.nullsPadding)
                    .setCompressionBlockSize(1)
                    .build();
        } catch (Exception e)
        {
            throw new RetinaException("Failed to create new writer", e);
        }
    }

    /*
     * flush buffer into disk file
     * if size is up maxbuffersize, close writer(will write file into disk) and create a new writer
     */
    private void flushBuffer(VectorizedRowBatch buffer) throws IOException
    {
        writer.addRowBatch(buffer);
        totalRowsWritten += this.pixelStride;

        synchronized (bufferLock)
        {
            buffer.reset();
            immutableBuffer = null;
            bufferLock.notifyAll();
        }

        if (totalRowsWritten >= maxBufferSize)
        {
            synchronized (bufferLock)
            {
                if (immutableBuffer != null)
                {
                    writer.addRowBatch(immutableBuffer);
                    immutableBuffer.reset();
                }
                writer.close();
                writer = null;
                totalRowsWritten = 0;
                try
                {
                    createNewWriter();
                } catch (RetinaException e)
                {
                    throw new IOException("Failed to create new writer", e);
                }
            }
        }
    }

    /**
     * collect resouces
     * @throws IOException
     */
    public void close() throws IOException
    {
        try
        {
            synchronized (bufferLock)
            {
                if (immutableBuffer != null)
                {
                    writer.addRowBatch(immutableBuffer);
                    immutableBuffer.reset();
                }
                if (activeBuffer != null)
                {
                    writer.addRowBatch(activeBuffer);
                    activeBuffer.reset();
                }
                writer.close();
            }
            flushExecutor.shutdown();
            try
            {
                if (!flushExecutor.awaitTermination(10, TimeUnit.SECONDS))
                {
                    flushExecutor.shutdownNow();
                }
            } catch (InterruptedException e)
            {
                flushExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        } catch (Exception e)
        {
            logger.error("Falied to close WriterBuffer ", e);
            throw new IOException("Falied to close WriterBuffer ", e);
        }
    }
}
