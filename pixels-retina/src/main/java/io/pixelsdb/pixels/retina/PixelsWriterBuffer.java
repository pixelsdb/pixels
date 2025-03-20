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
import static io.pixelsdb.pixels.common.utils.Constants.DEFAULT_HDFS_BLOCK_SIZE;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

public class PixelsWriterBuffer
{
    private static final Logger logger = LogManager.getLogger(PixelsWriterBuffer.class);

    private final TypeDescription schema;

    //
    private final VectorizedRowBatch buffer1;
    private final VectorizedRowBatch buffer2;

    private final AtomicReference<VectorizedRowBatch> activeBuffer;

    // 
    private final AtomicBoolean buffer1Active;
    private final AtomicBoolean buffer1Immutable;
    private final AtomicBoolean buffer2Immutable;

    private final AtomicInteger buffer1RowCount;
    private final AtomicInteger buffer2RowCount;

    private final int pixelStride;
    private final int rowGroupSize;
    private final long blockSize;
    private final short replication;
    private final EncodingLevel encodingLevel;
    private final boolean nullsPadding;
    private final int maxBufferSize;

    private final ExecutorService flushExecutor;

    public PixelsWriterBuffer(TypeDescription schema)
    {
        this.schema = schema;

        ConfigFactory configFactory = ConfigFactory.Instance();
        this.pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
        this.rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size"));
        this.blockSize = Long.parseLong(configFactory.getProperty("block.size"));
        this.replication = Short.parseShort(configFactory.getProperty("block.replication"));
        this.encodingLevel = EncodingLevel.from(Integer.parseInt(configFactory.getProperty("retina.buffer.flush.encodingLevel")));
        this.nullsPadding = Boolean.parseBoolean(configFactory.getProperty("retina.buffer.flush.nullsPadding"));
        this.maxBufferSize = Integer.parseInt(configFactory.getProperty("retina.buffer.flush.size"));

        this.buffer1 = schema.createRowBatchWithHiddenColumn(pixelStride, TypeDescription.Mode.NONE);
        this.buffer2 = schema.createRowBatchWithHiddenColumn(pixelStride, TypeDescription.Mode.NONE);
        this.activeBuffer = new AtomicReference<>(buffer1);
        this.buffer1Active = new AtomicBoolean(true);
        this.buffer1Immutable = new AtomicBoolean(false);
        this.buffer2Immutable = new AtomicBoolean(false);
        this.buffer1RowCount = new AtomicInteger(0);
        this.buffer2RowCount = new AtomicInteger(0);

        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "pixelsWriterBuffer");
            t.setDaemon(true);
            return t;
        });
    }

    public boolean addRow(String[] values, long timestamp)
    {
        checkArgument(values.length == schema.getChildren().size(),
                "Column values count does not match schema column count");

        VectorizedRowBatch buffer = activeBuffer.get();
        AtomicInteger rowCount = (buffer == buffer1) ? buffer1RowCount : buffer2RowCount;
        AtomicBoolean immutable = (buffer == buffer1) ? buffer1Immutable : buffer2Immutable;

        if (immutable.get() || rowCount.get() >= maxBufferSize) {
            if (!switchBuffer()) {
                return false;
            }

            buffer = activeBuffer.get();
            rowCount = (buffer == buffer1) ? buffer1RowCount : buffer2RowCount;
            immutable = (buffer == buffer1) ? buffer1Immutable : buffer2Immutable;
        }

        int rowIndex = rowCount.getAndIncrement();

        if (rowIndex >= maxBufferSize) {
            rowCount.decrementAndGet();
            return false;
        }

        ColumnVector[] columnVectors = buffer.cols;
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null) {
                columnVectors[i].noNulls = false;
                columnVectors[i].isNull[rowIndex] = true;
            } else {
                columnVectors[i].add(values[i]);
            }
        }

        columnVectors[columnVectors.length - 1].isNull[rowIndex] = false;
        columnVectors[columnVectors.length - 1].add(timestamp);

        buffer.size = Math.max(buffer.size, rowIndex + 1);

        if (rowCount.get() >= maxBufferSize) {
            switchBuffer();
        }

        return true;
    }
    
    private boolean switchBuffer()
    {
        boolean isBuffer1Active = buffer1Active.get();
        VectorizedRowBatch currentBuffer = isBuffer1Active ? buffer1 : buffer2;
        AtomicBoolean currentImmutable = isBuffer1Active ? buffer1Immutable : buffer2Immutable;
        AtomicInteger currentRowCount = isBuffer1Active ? buffer1RowCount : buffer2RowCount;

        // current buffer is still available
        if (!currentImmutable.get() && currentRowCount.get() < maxBufferSize) {
            return true;
        }

        AtomicBoolean otherImmutable = isBuffer1Active ? buffer2Immutable : buffer1Immutable;
        if (otherImmutable.get()) {
            return false; // another buffer is not available
        }

        currentImmutable.set(true);

        VectorizedRowBatch newBuffer = isBuffer1Active ? buffer2 : buffer1;
        activeBuffer.set(newBuffer);
        buffer1Active.set(!isBuffer1Active);

        final VectorizedRowBatch bufferToFlush = currentBuffer;
        final boolean isBuffer1 = isBuffer1Active;

        flushExecutor.submit(() -> {
            try 
            {
                flushBuffer(bufferToFlush, isBuffer1);
            } catch (IOException e) 
            {
                logger.error("Failed to flush buffer ", e);
            }
        });

        return true;
    }
    
    private void flushBuffer(VectorizedRowBatch buffer, boolean isBuffer1) throws IOException
    {
        if (buffer.size == 0) {
            if (isBuffer1) {
                buffer1Immutable.set(false);
                buffer1RowCount.set(0);
            } else {
                buffer1Immutable.set(false);
                buffer1RowCount.set(0);
            }
            return;
        }

        try {
            String targetDirPath = "";
            String targetFileName = DateUtil.getCurTime() + ".pxl";
            String targetFilePath = targetDirPath + targetFileName;

            Storage targetStorage = StorageFactory.Instance().getStorage(targetDirPath);

            PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder()
                    .setSchema(schema)
                    .setHasHiddenColumn(true)
                    .setPixelStride(pixelStride)
                    .setRowGroupSize(rowGroupSize)
                    .setStorage(targetStorage)
                    .setPath(targetFilePath)
                    .setBlockSize(blockSize)
                    .setReplication(replication)
                    .setBlockPadding(true)
                    .setEncodingLevel(encodingLevel)
                    .setNullsPadding(nullsPadding)
                    .setCompressionBlockSize(1)
                    .build();

            pixelsWriter.addRowBatch(buffer);
            pixelsWriter.close();

            buffer.reset();
            if (isBuffer1) {
                buffer1Immutable.set(false);
                buffer1RowCount.set(0);
            } else {
                buffer2Immutable.set(false);
                buffer1RowCount.set(0);
            }
        } catch (Exception e) {
            logger.error("Failed to flush buffer ", e);
            throw new IOException("Failed to flush buffer ", e);
        }
    }
    
    public void close() throws IOException
    {
        try
        {
            if (buffer1RowCount.get() > 0 && !buffer1Immutable.getAndSet(true))
            {
                flushBuffer(buffer1, true);
            }
    
            if (buffer2RowCount.get() > 0 && !buffer2Immutable.getAndSet(true))
            {
                flushBuffer(buffer2, false);
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
