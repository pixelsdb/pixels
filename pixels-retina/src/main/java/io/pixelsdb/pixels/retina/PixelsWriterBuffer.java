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
    private MemTable activeMemTable;

    // Wait to refresh to etcd
    private final List<MemTable> immutableMemTables;

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

        this.activeMemTable = new MemTable(schema, pixelStride, TypeDescription.Mode.NONE);
        this.immutableMemTables = new ArrayList<>();
        this.etcdEntries = new ArrayList<>();
        // Initialization adds reference counts to all data
        this.currentVersion = new SuperVersion(activeMemTable, immutableMemTables, etcdEntries);

        this.flushExecutor = Executors.newFixedThreadPool(2);
        startEtcdFlushScheduler();
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

        boolean added = false;
        while (!added) {
            this.versionLock.readLock().lock();
            added = this.activeMemTable.add(values, timestamp);
            this.versionLock.readLock().unlock();
            if (!added)
            {
                switchMemTable();
            }
        }
        return true;
    }

    private void switchMemTable()
    {
        this.versionLock.writeLock().lock();
        try
        {
            if (this.activeMemTable.isEmpty())
            {
                return;
            }

            /**
             * For activeMemTable, at initialization the reference count is 2 because of *this and superVersion
             * Here only currentVersion is destroyed, *this is still in use, so only one call to unref() is needed.
             */
            MemTable oldMemTable = this.activeMemTable;
            this.immutableMemTables.add(this.activeMemTable);
            this.activeMemTable = new MemTable(this.schema, this.pixelStride, TypeDescription.Mode.NONE);

            SuperVersion newVersion = new SuperVersion(this.activeMemTable, this.immutableMemTables, this.etcdEntries);
            SuperVersion oldVersion = this.currentVersion;
            this.currentVersion = newVersion;
            oldVersion.unref();

            triggerFlushToEtcd(oldMemTable);
        } finally
        {
            this.versionLock.writeLock().unlock();
        }
    }

    private void triggerFlushToEtcd(MemTable flushToEtcdMemTable)
    {
        flushExecutor.submit(() -> {
            try
            {
                // put etcd entry
                long id = this.etcdKeyIdCounter.getAndIncrement();
                String etcdEntryKey = this.schemaName + '_' + tableName + '_' + id;
                etcdUtil.putKeyValue(etcdEntryKey, new String(flushToEtcdMemTable.serialize())); // synchronous operation
                EtcdEntry etcdEntry = new EtcdEntry(id);
                etcdEntry.ref();

                // update SuperVersion
                versionLock.writeLock().lock();
                try
                {
                    this.immutableMemTables.remove(flushToEtcdMemTable);
                    this.etcdEntries.add(etcdEntry);

                    SuperVersion newVersion = new SuperVersion(this.activeMemTable, this.immutableMemTables, this.etcdEntries);

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
                flushToEtcdMemTable.unref();  // unref in the end
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
                    this.etcdEntries.removeAll(flushEtcdEntries);

                    SuperVersion oldVersion = this.currentVersion;
                    this.currentVersion = new SuperVersion(this.activeMemTable, this.immutableMemTables, this.etcdEntries);
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
                for (EtcdEntry etcdEntry: flushEtcdEntries)
                {
                    etcdEntry.unref(); // unref in the end
                }
            }
        });
    }

    private void createNewWriter() throws RetinaException
    {
        if (this.writer != null) {
            throw new RetinaException("Writer already exists");
        }
        try {
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
        } catch (Exception e) {
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
            for (MemTable immutableMemTable: sv.getImmutableMemTables())
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
            activeMemTable.unref();
            for (MemTable immutableMemTable: sv.getImmutableMemTables())
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
