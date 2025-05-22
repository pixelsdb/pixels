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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private final String schemaName;
    private final String tableName;

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

    // Active memTable
    private MemTable memTable;

    // Wait to refresh to etcd
    private final List<ImmutableMemTable> immutableMemTables;

    // Etcd key id counter
    private final AtomicInteger etcdKeyIdCounter = new AtomicInteger(0);

    // Etcd
    private final List<EtcdEntry> etcdEntries;
    private final EtcdUtil etcdUtil = EtcdUtil.Instance();

    // Current data view
    private volatile SuperVersion currentVersion;

    // Backend flush thread
    private final ExecutorService flushExecutor;

    // Lock for SuperVersion switch
    private final ReadWriteLock versionLock = new ReentrantReadWriteLock();

    // ETCD
    private static final long BUFFER_EXPIRY_TIME = 3600;

    public PixelsWriterBuffer(TypeDescription schema, String schemaName, String tableName,
                              String targetOrderedDirPath, String targetCompactDirPath) throws RetinaException
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
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

        this.memTable = new MemTable(schema, pixelStride, TypeDescription.Mode.NONE);
        this.immutableMemTables = new ArrayList<>();
        this.etcdEntries = new ArrayList<>();
        this.currentVersion = new SuperVersion(memTable, immutableMemTables, etcdEntries);

        this.flushExecutor = Executors.newFixedThreadPool(2);
        startEtcdFlushScheduler();
        startDiskFlushScheduler();
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
        int columnCount = this.schema.getChildren().size();
        checkArgument(values.length == columnCount,
                "Column values count does not match schema column count");

        this.versionLock.readLock().lock();
        try
        {
           // add into memTable
            boolean added = this.memTable.add(values, timestamp);
            if (!added || this.memTable.isFull())
            {
                switchMemTable();
                if (!added) // recursive call
                {
                    addRow(values, timestamp);
                }
            }
        } finally
        {
            this.versionLock.readLock().unlock();
        }

//        // activeBuffer is full, switch to immutable and flush it into etcd
//        if (activeBufferRowCount.get() >= pixelStride)
//        {
//            if (isWritingImmutableBufferToEtcd())
//            {
//                throw new RetinaException("Adding elements too quickly, before having time to save the last immutable buffer");
//            }
//
//            // switch active buffer to immutable buffer
//            setSwitchingActiveToImmutable();
//            immutableBuffer = activeBuffer;
//            activeBuffer = this.schema.createRowBatchWithHiddenColumn(pixelStride, TypeDescription.Mode.NONE);
//            activeBufferRowCount.set(0);
//            clearSwitchingActiveToImmutable();
//
//            etcdFlushExecutor.submit(() ->
//            {
//                setWritingImmutableBufferToEtcd();
//                String bufferKey = bufferKeyPrefix + "buffer_" + getEtcdEndIndex();
//                byte[] serializedBuffer = null; // = serializeBuffer(buffer);
//                totalBytesWritten += serializedBuffer.length;
//                etcdUtil.putKeyValue(bufferKey, new String(serializedBuffer));
//                long newState = finishPutBufferToEtcd();
//
//                // check weather need to write buffers in etcd into disk file
//                if (totalBytesWritten >= maxBufferSize)
//                {
//                    fileWriteExecutor.submit(() ->{
//                        if (writer == null)
//                        {
//                            try
//                            {
//                                createNewWriter();
//                            } catch (RetinaException e)
//                            {
//                                logger.error("Failed to create pixels writer", e);
//                            }
//                        }
//
//                        int startIndex = (int) ((newState & ETCD_START_INDEX_MASK) >> 24);
//                        int endIndex = (int) (newState & ETCD_END_INDEX_MASK);
//                        try
//                        {
//                            for (int i = startIndex; i < endIndex; ++i)
//                            {
//                                String curBufferKey = bufferKeyPrefix + "buffer_" + i;
//                                KeyValue value = etcdUtil.getKeyValue(curBufferKey);
//                                if (value != null)
//                                {
//                                    VectorizedRowBatch batch = null; //deserializeBuffer(value.getValue().toString());
//                                    if (batch != null)
//                                    {
//                                        writer.addRowBatch(batch);
//                                    }
//                                }
//                            }
//                            writer.close();
//                        } catch (IOException e)
//                        {
//                            logger.error("Failed to write buffer from etcd to disk file", e);
//                        }
//
//                        // TODO: atomicity switching metadata, update state and update totalBytesWritten
//                        setEtcdStartIndex(endIndex);
//                        for (int i = startIndex; i < endIndex; ++i)
//                        {
//                            String curBufferKey = bufferKeyPrefix + "buffer_" + i;
//                            etcdUtil.delete(bufferKey);
//                        }
//                        totalBytesWritten = 0;
//                    });
//                }
//            });
//        }
//
//        // TODO: ensure multi thread safe
//        for (int i = 0; i < values.length; ++i)
//        {
//            this.activeBuffer.cols[i].add(new String(values[i]));
//        }
//        this.activeBuffer.cols[columnCount].add(timestamp);
//        activeBuffer.size = activeBufferRowCount.incrementAndGet();
        return true;
    }

    private void createNewWriter() throws RetinaException
    {
        if (this.writer != null)
        {
            throw new RetinaException("Writer already exists");
        }
        try
        {
            String targetFileName = DateUtil.getCurTime() + ".pxl";
            String targetFilePath = this.targetOrderedDirPath + targetFileName;
            this.writer = PixelsWriterImpl.newBuilder()
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

    private void switchMemTable()
    {
        this.versionLock.writeLock().lock();
        try
        {
            if (this.memTable.isEmpty())
            {
                return;
            }

            ImmutableMemTable immutableMemTable = this.memTable.markImmutable();
            this.immutableMemTables.add(immutableMemTable);

            MemTable newMemTable = new MemTable(this.schema, this.pixelStride, TypeDescription.Mode.NONE);
            SuperVersion newVersion = new SuperVersion(newMemTable, this.immutableMemTables, this.etcdEntries);

            SuperVersion oldVersion = this.currentVersion;
            this.currentVersion = newVersion;
            oldVersion.unref();

            MemTable oldMemTable = this.memTable;
            this.memTable = newMemTable;
            oldMemTable.unref();

            triggerFlushToEtcd(immutableMemTable);
        } finally
        {
            this.versionLock.writeLock().unlock();
        }
    }

    private void triggerFlushToEtcd(ImmutableMemTable immutableMemTable)
    {
        immutableMemTable.ref();
        flushExecutor.submit(() -> {
            try
            {
                // put etcd entry
                long id = this.etcdKeyIdCounter.getAndIncrement();
                String etcdEntryKey = this.schemaName + '_' + tableName + '_' + id;
                etcdUtil.putKeyValue(etcdEntryKey, new String(immutableMemTable.serialize())); // synchronous operation
                EtcdEntry etcdEntry = new EtcdEntry(id);
                etcdEntry.ref();

                // update SuperVersion
                versionLock.writeLock().lock();
                try
                {
                    List<ImmutableMemTable> newImmutableMemTables = new ArrayList<>(currentVersion.getImmutableMemTables());
                    newImmutableMemTables.remove(immutableMemTable);
                    List<EtcdEntry> newEtcdEntries = new ArrayList<>(this.currentVersion.getEtcdEntries());
                    newEtcdEntries.add(etcdEntry);
                    SuperVersion newVersion = new SuperVersion(this.currentVersion.getMemTable(), newImmutableMemTables, newEtcdEntries);

                    SuperVersion oldVersion = this.currentVersion;
                    this.currentVersion = newVersion;
                    oldVersion.unref();
                } finally
                {
                    versionLock.writeLock().unlock();
                }
            } catch (Exception e)
            {
                throw new RuntimeException("Failed to flush to etcd ", e);
            } finally
            {
                immutableMemTable.unref();
            }
        });
    }

    private void startEtcdFlushScheduler()
    {
        Thread scheduler = new Thread(() -> {
            while(!Thread.currentThread().isInterrupted())
            {
                try
                {
                    Thread.sleep(3600);
                    SuperVersion sv = getCurrentVersion();
                    try
                    {
                        List<EtcdEntry> etcdEntries = sv.getEtcdEntries();
                        if (etcdEntries.size() >= 100)
                        {
                            flushEtcdToDisk(etcdEntries);
                        }
                    } finally
                    {
                        sv.unref();
                    }
                } catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e)
                {
                    logger.error("Error in Etcd flush scheduler: ", e);
                }
            }
        });
        scheduler.setDaemon(true);
        scheduler.start();
    }

    private void startDiskFlushScheduler()
    {

    }

    private void flushEtcdToDisk(List<EtcdEntry> flushEtcdEntries)
    {
        this.flushExecutor.submit(() -> {
            try
            {
                if (this.writer == null)
                {
                    createNewWriter();
                }
                for (EtcdEntry etcdEntry: flushEtcdEntries)
                {
                    String etcdEntryKey = this.schemaName + '_' + tableName + '_' + etcdEntry.getId();
                    byte[] data = this.etcdUtil.getKeyValue(etcdEntryKey).getValue().getBytes();
                    this.writer.addRowBatch(VectorizedRowBatch.deserialize(data));
                }
                this.writer.close();

                // update super version
                this.versionLock.writeLock().lock();
                try
                {
                    List<EtcdEntry> newEtcdEntries = new ArrayList<>(this.currentVersion.getEtcdEntries());
                    newEtcdEntries.removeAll(flushEtcdEntries);

                    SuperVersion newVersion = new SuperVersion(this.currentVersion.getMemTable(), this.currentVersion.getImmutableMemTables(), newEtcdEntries);
                    SuperVersion oldVersion = this.currentVersion;
                    this.currentVersion = newVersion;
                    oldVersion.unref();
                } finally
                {
                    this.versionLock.writeLock().unlock();
                }
            } catch (Exception e)
            {
                logger.error("Error in flushEtcdToDisk: ", e);
            } finally
            {
                for(EtcdEntry etcdEntry: flushEtcdEntries)
                {
                    etcdEntry.unref();
                }
            }
        });
    }

    /**
     * get current version
     * caller must call unref()
     * @return
     */
    public SuperVersion getCurrentVersion()
    {
        versionLock.readLock().lock();
        try
        {
            currentVersion.ref();
            return currentVersion;
        } finally
        {
            versionLock.readLock().unlock();
        }
    }

    /**
     * collect resouces
     *
     * @throws IOException
     */
    public void close() throws IOException
    {
        SuperVersion sv = getCurrentVersion();
        try
        {
            if (this.writer == null)
            {
                createNewWriter();
            }

            // add memtable to writer
            this.writer.addRowBatch(sv.getMemTable().getRowBatch());

            // add immutable memtable to writer
            for (ImmutableMemTable immutableMemTable: sv.getImmutableMemTables())
            {
                this.writer.addRowBatch(immutableMemTable.getRowBatch());
            }

            // add etcd entries to writer
            for (EtcdEntry etcdEntry: sv.getEtcdEntries())
            {
                String etcdEntryKey = this.schemaName + '_' + tableName + '_' + etcdEntry.getId();
                byte[] data = this.etcdUtil.getKeyValue(etcdEntryKey).getValue().getBytes();
                this.writer.addRowBatch(VectorizedRowBatch.deserialize(data));
            }

            this.writer.close();
        } catch (Exception e)
        {
            logger.error("Error in close: ", e);
        } finally
        {
            sv.unref();
            memTable.unref();
            for (ImmutableMemTable immutableMemTable: sv.getImmutableMemTables())
            {
                immutableMemTable.unref();
            }
            for (EtcdEntry etcdEntry: sv.getEtcdEntries())
            {
                etcdEntry.unref();
            }
            this.flushExecutor.shutdown();
        }
    }
}
