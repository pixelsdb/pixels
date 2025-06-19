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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;

import static io.pixelsdb.pixels.storage.s3.Minio.ConfigMinio;

/**
 * Data flows from the CDC into pixels, where it is first written to
 * the writer buffer and becomes visible externally. Each logical table
 * corresponds to a writer buffer, which contains only one memTable responsible
 * for writing data. Once the memTable is full, data is written to an immutable
 * memTable, and multiple immutable memTables can exist. Currently, to simplify
 * the distributed design, the immutable memTable is first written to the shared
 * storage minio, and then the data in minio is dumped to a disk file.
 *  -----        --------------------------------      -------      -----------
 * | CDC | ===> | memTable -> immutable memTable | -> | minio | -> | disk file |
 *  -----        --------------------------------      -------      -----------
 */
public class PixelsWriterBuffer
{
    private static final Logger logger = LogManager.getLogger(PixelsWriterBuffer.class);

    private final String schemaName;
    private final String tableName;

    // Column information is recorded to create rowBatch.
    private final TypeDescription schema;

    // Configuration information of PixelsWriter
    private final int pixelStride;
    private final long blockSize;
    private final short replication;
    private final EncodingLevel encodingLevel;
    private final boolean nullsPadding;
    private final int maxBufferSize;
    private final Path targetOrderedDirPath;
    private final Path targetCompactDirPath;
    private final Storage targetOrderedStorage;
    private final Storage targetCompactStorage;

    /**
     * Allocate unique identifier for data (MemTable/MinioEntry)
     * There is no need to use atomic variables because
     * there are write locks in all concurrent situations.
     */
    private long idCounter = 0L;

    // Active memTable
    private MemTable activeMemTable;

    // Wait to refresh to shared storage
    private final List<MemTable> immutableMemTables;

    // minio
    private final Storage minio;
    private final List<ObjectEntry> objectEntries;

    // Current data view
    private volatile SuperVersion currentVersion;

    // Backend flush thread
    private final ExecutorService flushMinioExecutor;
    private final ExecutorService flushDiskExecutor;

    // Lock for SuperVersion switch
    private final ReadWriteLock versionLock = new ReentrantReadWriteLock();

    /**
     * The mapping from rowId to record position
     */
    private final Map<Long, RecordLocation> recordLocationMap;

    /**
     * The mapping from rowBatch location to rowBatch visibility
     */
    private final Map<Long, RGVisibility> visibilityMap;

    private long fileId;   // current fileId
    private long currentBufferSize;
    private final List<FileWriterManager> fileWriterManagers;
    private FileWriterManager currentFileWriterManager;
    private ReentrantLock bufferSizeLock;

    public PixelsWriterBuffer(TypeDescription schema, String schemaName, String tableName,
                              Path targetOrderedDirPath, Path targetCompactDirPath) throws RetinaException
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.schema = schema;

        ConfigFactory configFactory = ConfigFactory.Instance();
        this.pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
        this.targetOrderedDirPath = targetOrderedDirPath;
        this.targetCompactDirPath = targetCompactDirPath;
        try
        {
            this.targetOrderedStorage = StorageFactory.Instance().getStorage(targetOrderedDirPath.getUri());
            this.targetCompactStorage = StorageFactory.Instance().getStorage(targetCompactDirPath.getUri());
        } catch (Exception e)
        {
            logger.error("Failed to get storage", e);
            throw new RetinaException("Failed to get storage", e);
        }
        this.blockSize = Long.parseLong(configFactory.getProperty("block.size"));
        this.replication = Short.parseShort(configFactory.getProperty("block.replication"));
        this.encodingLevel = EncodingLevel.from(Integer.parseInt(configFactory.getProperty("retina.buffer.flush.encodingLevel")));
        this.nullsPadding = Boolean.parseBoolean(configFactory.getProperty("retina.buffer.flush.nullsPadding"));
        this.maxBufferSize = Integer.parseInt(configFactory.getProperty("retina.buffer.flush.size"));

        this.activeMemTable = new MemTable(this.idCounter, schema, pixelStride, TypeDescription.Mode.NONE);
        this.immutableMemTables = new ArrayList<>();
        this.objectEntries = new ArrayList<>();

        // Initialization adds reference counts to all data
        this.currentVersion = new SuperVersion(activeMemTable, immutableMemTables, objectEntries);

        this.flushMinioExecutor = Executors.newFixedThreadPool(1);
        this.flushDiskExecutor = Executors.newFixedThreadPool(1);

        this.recordLocationMap = new ConcurrentHashMap<>();
        this.visibilityMap = new ConcurrentHashMap<>();

        // init first visibility for active memTable
        RGVisibility visibility = new RGVisibility(pixelStride);
        this.visibilityMap.put(this.idCounter, visibility);
        this.idCounter++;

        this.currentBufferSize = 0;
        this.bufferSizeLock = new ReentrantLock();
        this.fileWriterManagers = new ArrayList<>();

        // minio
        try
        {
            ConfigMinio(configFactory.getProperty("minio.region"),
                    configFactory.getProperty("minio.endpoint"),
                    configFactory.getProperty("minio.access.key"),
                    configFactory.getProperty("minio.secret.key"));
            this.minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
        } catch (IOException e)
        {
            logger.error("error when config minio ", e);
            throw new RetinaException("error when config minio ", e);
        }

        this.currentFileWriterManager = new FileWriterManager(this.minio,
                this.schemaName, this.tableName, this.schema,
                this.targetOrderedDirPath, this.targetOrderedStorage,
                this.pixelStride, this.blockSize, this.replication,
                this.encodingLevel, this.nullsPadding, 0);

        startFlushMinioToDiskScheduler();
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

        long rowId = -1;
        while (rowId < 0) {
            RecordLocation recordLocation = new RecordLocation();
            this.versionLock.readLock().lock();
            rowId = this.activeMemTable.add(values, timestamp, recordLocation);
            // add block id, need read activeMemTable.id
            this.versionLock.readLock().unlock();
            recordLocationMap.put(rowId, recordLocation);

            if (rowId == -1)  // active memTable is full
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
            if (!this.activeMemTable.isFull())
            {
                return;
            }

            this.bufferSizeLock.lock();
            this.currentBufferSize += this.activeMemTable.getSize();
            if (this.currentBufferSize >= this.maxBufferSize)
            {
                long id = this.activeMemTable.getId();
                this.currentBufferSize = 0;
                this.currentFileWriterManager.setLastBlockId(id);
                this.fileWriterManagers.add(this.currentFileWriterManager);
                this.currentFileWriterManager = new FileWriterManager(this.minio,
                        this.schemaName, this.tableName, this.schema,
                        this.targetOrderedDirPath, this.targetOrderedStorage,
                        this.pixelStride, this.blockSize, this.replication,
                        this.encodingLevel, this.nullsPadding, this.idCounter);
            }
            this.bufferSizeLock.unlock();

            /**
             * For activeMemTable, at initialization the reference count is 2 because of *this and superVersion
             * Here only currentVersion is destroyed, *this is still in use, so only one call to unref() is needed.
             */
            MemTable oldMemTable = this.activeMemTable;
            this.immutableMemTables.add(this.activeMemTable);
            this.activeMemTable = new MemTable(this.idCounter, this.schema, this.pixelStride, TypeDescription.Mode.NONE);
            RGVisibility visibility = new RGVisibility(pixelStride);
            this.visibilityMap.put(this.idCounter, visibility);
            this.idCounter++;

            SuperVersion newVersion = new SuperVersion(this.activeMemTable, this.immutableMemTables, this.objectEntries);
            SuperVersion oldVersion = this.currentVersion;
            this.currentVersion = newVersion;
            oldVersion.unref();

            triggerFlushToMinio(oldMemTable);
        } catch (Exception e)
        {
            logger.error("Failed to create switch memTable", e);
            throw new RuntimeException("Failed to create switch memTable", e);
        } finally
        {
            this.versionLock.writeLock().unlock();
        }
    }

    private void triggerFlushToMinio(MemTable flushMemTable)
    {
        flushMinioExecutor.submit(() -> {
            try
            {
                // put into minio
                long id = flushMemTable.getId();
                writeIntoMinio(this.schemaName + '/' + this.tableName + '/' + id,
                        flushMemTable.serialize());

                ObjectEntry objectEntry = new ObjectEntry(id, flushMemTable.getSize());
                objectEntry.ref();

                // update SuperVersion
                versionLock.writeLock().lock();
                try
                {
                    this.immutableMemTables.remove(flushMemTable);
                    this.objectEntries.add(objectEntry);

                    SuperVersion newVersion = new SuperVersion(this.activeMemTable, this.immutableMemTables, this.objectEntries);

                    SuperVersion oldVersion = this.currentVersion;
                    this.currentVersion = newVersion;
                    oldVersion.unref();
                } finally
                {
                    versionLock.writeLock().unlock();
                }
            } catch (Exception e)
            {
                throw new RuntimeException("Failed to flush to minio ", e);
            } finally
            {
                flushMemTable.unref();  // unref in the end
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
     * Determine whether the last data block managed by fileWriterManager has
     * been written to minio. If it has been written, execute the file write
     * operation and delete the corresponding ObjectEntry in the unified view.
     */
    private void startFlushMinioToDiskScheduler()
    {
        Thread scheduler = new Thread(() -> {
            try
            {
                for (FileWriterManager fileWriterManager : this.fileWriterManagers)
                {
                    String lastMinioEntry = this.schemaName + '/' + this.tableName + '/' + fileWriterManager.getLastBlockId();
                    if (this.minio.exists(lastMinioEntry))
                    {
                        fileWriterManager.finish();

                        // update super version
                        this.versionLock.writeLock().lock();
                        Set<Long> idsToRemove = LongStream.rangeClosed(fileWriterManager.getFirstBlockId(),
                                fileWriterManager.getLastBlockId()).boxed().collect(Collectors.toSet());
                        List<ObjectEntry> toRemove = this.objectEntries.stream()
                                .filter(objectEntry -> idsToRemove.contains(objectEntry.getId()))
                                .collect(Collectors.toList());

                        this.objectEntries.removeAll(toRemove);

                        SuperVersion oldVersion = this.currentVersion;
                        this.currentVersion = new SuperVersion(this.activeMemTable, this.immutableMemTables, this.objectEntries);
                        oldVersion.unref();
                        this.versionLock.writeLock().unlock();

                        for (ObjectEntry objectEntry : toRemove)
                        {
                            objectEntry.unref();
                        }
                    }
                }
            } catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
        scheduler.setDaemon(true);
        scheduler.start();
    }

    private void writeIntoMinio(String filePath, byte[] data)
    {
        try
        {
            PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(
                    this.minio, filePath, true);
            writer.append(data, 0, data.length);
            writer.close();
        } catch (IOException e)
        {
            logger.error("failed to write data into minio: ", e);
        }
    }

    /**
     * collect resouces
     *
     * @throws IOException
     */
    public void close() throws IOException
    {
        // First, shut down the flush process to prevent changes to the data view.
        this.flushMinioExecutor.shutdown();
        this.flushDiskExecutor.shutdown();

        SuperVersion sv = getCurrentVersion();
        try
        {
            // add memtable to writer
            this.currentFileWriterManager.addRowBatch(sv.getMemTable().getRowBatch());

            // add immutable memtable to writer
            for (MemTable immutableMemTable: sv.getImmutableMemTables())
            {
                this.currentFileWriterManager.addRowBatch(immutableMemTable.getRowBatch());
            }

            this.currentFileWriterManager.finish();

            // handle fileWriterManager that has not yet been written to the file
            for (FileWriterManager fileWriterManager : this.fileWriterManagers)
            {
                fileWriterManager.finish();
            }
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
            for (ObjectEntry objectEntry : sv.getObjectEntries())
            {
                objectEntry.unref();
                this.minio.delete(this.schemaName + '/' + this.tableName + '/' + objectEntry.getId(), false);
            }
        }
    }
}
