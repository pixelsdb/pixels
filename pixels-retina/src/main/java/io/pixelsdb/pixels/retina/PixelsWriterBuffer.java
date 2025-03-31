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
import java.util.concurrent.atomic.AtomicLong;

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
    private final AtomicInteger activeBufferRowCount = new AtomicInteger(0);
    private long totalBytesWritten = 0;
    private final String bufferKeyPrefix;
    private final String bufferWritingFlagKey;

    // State flag
    private final AtomicLong state = new AtomicLong(0);
    private static final long ACTIVE_TO_IMMUTABLE_SWITCH_MASK = 0xFF00000000000000L;
    private static final long IMMUTABLE_BUFFER_ETCD_WRITE_MASK = 0x00FF000000000000L;
    private static final long ETCD_START_INDEX_MASK = 0x0000FFFFFF000000L;
    private static final long ETCD_END_INDEX_MASK = 0x0000000000FFFFFFL;

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

        this.etcdFlushExecutor = Executors.newSingleThreadExecutor(r ->
        {
            Thread t = new Thread(r, "pixels-etcd-flush");
            t.setDaemon(true);
            return t;
        });

        this.fileWriteExecutor = Executors.newSingleThreadExecutor(r ->
        {
            Thread t = new Thread(r, "pixels-file-writer");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * values is all column values, add values and timestamp into buffer
     *
     * @param values
     * @param timestamp
     * @return
     */
    public boolean addRow(byte[][] values, long timestamp) throws RetinaException
    {
        int columnCount = schema.getChildren().size();
        checkArgument(values.length == columnCount,
                "Column values count does not match schema column count");

        // activeBuffer is full, switch to immutable and flush it into etcd
        if (activeBufferRowCount.get() >= pixelStride)
        {
            if (isWritingImmutableBufferToEtcd())
            {
                throw new RetinaException("Adding elements too quickly, before having time to save the last immutable buffer");
            }

            // switch active buffer to immutable buffer
            setSwitchingActiveToImmutable();
            immutableBuffer = activeBuffer;
            activeBuffer = this.schema.createRowBatchWithHiddenColumn(pixelStride, TypeDescription.Mode.NONE);
            activeBufferRowCount.set(0);
            clearSwitchingActiveToImmutable();

            etcdFlushExecutor.submit(() ->
            {
                setWritingImmutableBufferToEtcd();
                String bufferKey = bufferKeyPrefix + "buffer_" + getEtcdEndIndex();
                byte[] serializedBuffer = null; // = serializeBuffer(buffer);
                totalBytesWritten += serializedBuffer.length;
                etcdUtil.putKeyValue(bufferKey, new String(serializedBuffer));
                long newState = finishPutBufferToEtcd();

                // check weather need to write buffers in etcd into disk file
                if (totalBytesWritten >= maxBufferSize)
                {
                    fileWriteExecutor.submit(() ->{
                        if (writer == null)
                        {
                            try
                            {
                                createNewWriter();
                            } catch (RetinaException e)
                            {
                                logger.error("Failed to create pixels writer", e);
                            }
                        }

                        int startIndex = (int) ((newState & ETCD_START_INDEX_MASK) >> 24);
                        int endIndex = (int) (newState & ETCD_END_INDEX_MASK);
                        try
                        {
                            for (int i = startIndex; i < endIndex; ++i)
                            {
                                String curBufferKey = bufferKeyPrefix + "buffer_" + i;
                                KeyValue value = etcdUtil.getKeyValue(curBufferKey);
                                if (value != null)
                                {
                                    VectorizedRowBatch batch = null; //deserializeBuffer(value.getValue().toString());
                                    if (batch != null)
                                    {
                                        writer.addRowBatch(batch);
                                    }
                                }
                            }
                            writer.close();
                        } catch (IOException e)
                        {
                            logger.error("Failed to write buffer from etcd to disk file", e);
                        }

                        // TODO: TODO: atomicity switching metadata, update state and update totalBytesWritten
                        setEtcdStartIndex(endIndex);
                        for (int i = startIndex; i < endIndex; ++i)
                        {
                            String curBufferKey = bufferKeyPrefix + "buffer_" + i;
                            etcdUtil.delete(bufferKey);
                        }
                        totalBytesWritten = 0;
                    });
                }
            });
        }

        // TODO: ensure multi thread safe
        for (int i = 0; i < values.length; ++i)
        {
            this.activeBuffer.cols[i].add(new String(values[i]));
        }
        this.activeBuffer.cols[columnCount].add(timestamp);
        activeBuffer.size = activeBufferRowCount.incrementAndGet();
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

    /**
     * collect resouces
     *
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

    public boolean isSwitchingActiveToImmutable()
    {
        return (state.get() & ACTIVE_TO_IMMUTABLE_SWITCH_MASK) != 0;
    }

    public void setSwitchingActiveToImmutable()
    {
        state.updateAndGet(s -> s | ACTIVE_TO_IMMUTABLE_SWITCH_MASK);
    }

    public void clearSwitchingActiveToImmutable()
    {
        state.updateAndGet(s -> s & ~ACTIVE_TO_IMMUTABLE_SWITCH_MASK);
    }

    public boolean isWritingImmutableBufferToEtcd()
    {
        return (state.get() & IMMUTABLE_BUFFER_ETCD_WRITE_MASK) != 0;
    }

    public void setWritingImmutableBufferToEtcd()
    {
        state.updateAndGet(s -> s | IMMUTABLE_BUFFER_ETCD_WRITE_MASK);
    }

    public void clearWritingImmutableBufferToEtcd()
    {
        state.updateAndGet(s -> s & ~IMMUTABLE_BUFFER_ETCD_WRITE_MASK);
    }

    public int getEtcdStartIndex()
    {
        return (int) ((state.get() & ETCD_START_INDEX_MASK) >> 24);
    }

    public void setEtcdStartIndex(int startIndex)
    {
        state.updateAndGet(s -> (s & ~ETCD_START_INDEX_MASK) | ((long) startIndex << 24));
    }

    public int getEtcdEndIndex()
    {
        return (int) (state.get() & ETCD_END_INDEX_MASK);
    }

    public long finishPutBufferToEtcd()
    {
        // increase end index and clear WritingImmutableBufferToEtcd flag
        return state.updateAndGet(s -> (s & ~ETCD_END_INDEX_MASK & ~IMMUTABLE_BUFFER_ETCD_WRITE_MASK)
                | ((s & ETCD_END_INDEX_MASK) + 1));
    }
}
