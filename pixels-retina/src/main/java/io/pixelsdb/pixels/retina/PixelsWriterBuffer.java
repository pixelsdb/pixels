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
import java.util.concurrent.atomic.AtomicInteger;

import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
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
    private AtomicInteger activeBufferRowCount = new AtomicInteger(0);
    private long totaolBytesWritten = 0;
    private final String bufferKeyPrefix;
    private final String bufferWritingFlagKey;
    private AtomicInteger etcdBufferStartIndex = new AtomicInteger(0);

    // State flag
    private AtomicInteger immutableBufferState = new AtomicInteger(0);
    private AtomicInteger etcdBufferState = new AtomicInteger(0);
    private static final int BUFFER_WRITING_FLAG_MASK = 0xFF000000;
    private static final int BUFFER_READER_COUNT_MASK = 0x00FFFFFF;
    private static final int ETCD_WRITING_FLAG_MASK = 0xFF000000;
    private static final int ETCD_BUFFER_COUNT_MASK = 0x00FFFFFF;

    // Backend Flush
    private final ExecutorService etcdFlushExecutor;
    private final ExecutorService fileWriteExecutor;

    // ETCD
    private final EtcdUtil etcdUtil = EtcdUtil.Instance();
    private static final long BUFFER_EXPIRY_TIME = 3600;

    public PixelsWriterBuffer(TypeDescription schema, String schemaName, String tableName,
            String targetOrderedDirPath, String targetCompactDirPath) throws RetinaException
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

        this.bufferKeyPrefix = schemaName + "/" + tableName + "/";
        this.bufferWritingFlagKey = bufferKeyPrefix + "writing_flag";
        try
        {
            etcdUtil.putKeyValue(bufferWritingFlagKey, "false");
        } catch (Exception e)
        {
            throw new RetinaException("Failed to initialize etcd writing flag", e);
        }

        this.etcdFlushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "pixels-etcd-flush");
            t.setDaemon(true);
            return t;
        });

        this.fileWriteExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "pixels-file-writer");
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

        // activeBuffer is full, switch to immutable and flush it into etcd
        if (activeBufferRowCount.get() >= pixelStride)
        {
            int oldState = immutableBufferState.getAndUpdate(state -> state | BUFFER_WRITING_FLAG_MASK);
            if ((oldState & BUFFER_WRITING_FLAG_MASK) != 0)
            {
                logger.error("Adding elements too quickly, before having time to save the last immutable buffer");
                return false;
            }

            try
            {
                // wait for all readers to finish
                while ((immutableBufferState.get() & BUFFER_READER_COUNT_MASK) != 0)
                {
                    logger.warn("Waiting for readers to finish");
                    Thread.sleep(100);
                }

                // switch active buffer to immutable buffer
                immutableBuffer = activeBuffer;
                activeBuffer = this.schema.createRowBatchWithHiddenColumn(pixelStride, TypeDescription.Mode.NONE);
                activeBufferRowCount.set(0);

                // clear the buffer writing flag
                immutableBufferState.updateAndGet(state -> state & ~BUFFER_WRITING_FLAG_MASK);

                // flush immutable buffer to etcd
                etcdFlushExecutor.submit(() -> {
                   try
                   {
                        immutableBufferState.updateAndGet(state -> (state & ~BUFFER_READER_COUNT_MASK) | ((state & BUFFER_READER_COUNT_MASK) + 1));
                        flushBufferToEtcd(immutableBuffer);
                   } catch (IOException e)
                   {
                       logger.error("Failed to flush buffer to etcd", e);
                   } finally
                   {
                       immutableBufferState.updateAndGet(state -> (state & ~BUFFER_READER_COUNT_MASK) | ((state & BUFFER_READER_COUNT_MASK) - 1));
                   }
                });
            } catch (Exception e)
            {
                throw new RuntimeException("Failed to swich active buffer to immutable buffer", e);
            }
        }

        activeBufferRowCount.incrementAndGet();
        activeBuffer.size++;
        for (int i = 0; i < values.length; ++i)
        {
            this.activeBuffer.cols[i].add(new String(values[i]));
        }
        this.activeBuffer.cols[columnCount].add(timestamp);
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
    private void flushBufferToEtcd(VectorizedRowBatch buffer) throws IOException
    {
        // marks immutable buffer being put into etcd.
        int bufferIndex = etcdBufferState.getAndUpdate(state -> (state & ~ETCD_WRITING_FLAG_MASK) | ((state & ETCD_BUFFER_COUNT_MASK) + 1));

        String bufferKey = bufferKeyPrefix + "buffer_" + bufferIndex;
        byte[] serializedBuffer = null; // = serializeBuffer(buffer);
        totaolBytesWritten += serializedBuffer.length;

        // put immutable buffer into etcd
        etcdUtil.putKeyValueWithExpireTime(bufferKey, new String(serializedBuffer), BUFFER_EXPIRY_TIME);
        // clear the writing flag and increment the buffer count, return the buffer count
        int newEtcdBufferCount = etcdBufferState.updateAndGet(state ->
                (state & ~ETCD_WRITING_FLAG_MASK) | ((state & ETCD_BUFFER_COUNT_MASK) + 1)
        ) & ETCD_BUFFER_COUNT_MASK;

        // check weather need to write buffer into disk file
        if (totaolBytesWritten >= maxBufferSize)
        {
            fileWriteExecutor.submit(() -> {
                try
                {
                    flushEtcdBuffersToFile(newEtcdBufferCount);
                } catch (Exception e)
                {
                    logger.error("Failed to flush etcd buffers to file", e);
                }
            });
        }
    }

    private void flushEtcdBuffersToFile(int bufferCount) throws RetinaException
    {
        try
        {
            if (writer == null)
            {
                createNewWriter();
                int bufferKeyIndex = etcdBufferStartIndex.get();
                while (bufferKeyIndex < bufferCount)
                {
                    String bufferKey = bufferKeyPrefix + "buffer_" + bufferKeyIndex;
                    KeyValue value = etcdUtil.getKeyValue(bufferKey);
                    if (value != null)
                    {
                        VectorizedRowBatch batch = null; //deserializeBuffer(value.getValue().toString());
                        if (batch != null)
                        {
                            writer.addRowBatch(batch);
                        }
                    }
                    bufferKeyIndex++;
                }
                writer.close();

                // TODO: atomicity switching metadata, update etcd buffer start index and buffer count
                bufferKeyIndex = etcdBufferStartIndex.getAndSet(bufferCount);
                int finalBufferKeyIndex = bufferKeyIndex;
                etcdBufferState.updateAndGet(state -> (state & ~ETCD_BUFFER_COUNT_MASK) | ((bufferCount - finalBufferKeyIndex) & ETCD_BUFFER_COUNT_MASK));

                // delete buffer from etcd
                while (bufferKeyIndex < bufferCount)
                {
                    String bufferKey = bufferKeyPrefix + "buffer_" + bufferKeyIndex;
                    etcdUtil.delete(bufferKey);
                }

                totaolBytesWritten = 0;
            }
        } catch (Exception e)
        {
            throw new RetinaException("Failed to flush etcd buffers to file", e);
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
            if (immutableBuffer != null)
            {
                if (writer == null)
                {
                    createNewWriter();
                }
                writer.addRowBatch(immutableBuffer);
                immutableBuffer = null;
            }
            if (activeBuffer != null)
            {
                if (writer == null)
                {
                    createNewWriter();
                }
                writer.addRowBatch(activeBuffer);
                activeBuffer = null;
            }
            if (writer != null)
            {
                writer.close();
                writer = null;
            }

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
        } catch (Exception e)
        {
            logger.error("Falied to close WriterBuffer ", e);
            throw new IOException("Falied to close WriterBuffer ", e);
        }
    }
}
