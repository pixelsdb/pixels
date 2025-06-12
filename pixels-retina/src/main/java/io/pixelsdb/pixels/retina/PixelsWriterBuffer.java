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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private final ExecutorService flushExecutor;

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

    /**
     * Write several data blocks to the same file and determine this when adding data.
     * At the same time, record the ID of the last data block in each file and determine
     * whether to trigger writing to the disk file when writing to distributed storage.
     */
    private long fileId;                        // current fileId
    private final List<Integer> fileSplitIds;   // The ID is for the data block.

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

        this.activeMemTable = new MemTable(this.idCounter, schema, pixelStride, TypeDescription.Mode.NONE);
        this.immutableMemTables = new ArrayList<>();
        this.objectEntries = new ArrayList<>();
        // Initialization adds reference counts to all data
        this.currentVersion = new SuperVersion(activeMemTable, immutableMemTables, objectEntries);

        this.flushExecutor = Executors.newFixedThreadPool(2);
        startMinioFlushScheduler();

        this.recordLocationMap = new HashMap<>();
        this.visibilityMap = new HashMap<>();

        // init first visibility for active memTable
        RGVisibility visibility = new RGVisibility(pixelStride);
        this.visibilityMap.put(this.idCounter, visibility);
        this.idCounter++;

        this.fileSplitIds = new ArrayList<>();

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

            //
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
            this.activeMemTable = new MemTable(this.idCounter++, this.schema, this.pixelStride, TypeDescription.Mode.NONE);

            SuperVersion newVersion = new SuperVersion(this.activeMemTable, this.immutableMemTables, this.objectEntries);
            SuperVersion oldVersion = this.currentVersion;
            this.currentVersion = newVersion;
            oldVersion.unref();

            triggerFlushToMinio(oldMemTable);
        } finally
        {
            this.versionLock.writeLock().unlock();
        }
    }

    private void triggerFlushToMinio(MemTable flushMemTable)
    {
        flushExecutor.submit(() -> {
            try
            {
                // put into minio
                long id = flushMemTable.getId();
                writeIntoMinio(this.schemaName + '/' + this.tableName + '/' + id,
                        flushMemTable.serialize());

                ObjectEntry objectEntry = new ObjectEntry(id);
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

    private void startMinioFlushScheduler()
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
                        List<ObjectEntry> objectEntries = sv.getObjectEntries();
                        if (objectEntries.size() >= 100)
                        {
                            flushMinioToDisk(objectEntries);
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
                    logger.error("Error in Minio flush scheduler: ", e);
                }
            }
        });
        scheduler.setDaemon(true);
        scheduler.start();
    }

    private void flushMinioToDisk(List<ObjectEntry> flushEntries)
    {
        this.flushExecutor.submit(() -> {
            try
            {
                if (this.writer == null)
                {
                    createNewWriter();
                }
                for (ObjectEntry objectEntry : flushEntries)
                {
                    String minioEntryKey = this.schemaName + '/' + tableName + '/' + objectEntry.getId();
                    PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(this.minio, minioEntryKey);
                    int length = (int) reader.getFileLength();
                    byte[] data = new byte[length];
                    reader.readFully(data, 0, length);
                    this.writer.addRowBatch(VectorizedRowBatch.deserialize(data));
                }
                this.writer.close();

                // update super version
                this.versionLock.writeLock().lock();
                try
                {
                    this.objectEntries.removeAll(flushEntries);

                    SuperVersion oldVersion = this.currentVersion;
                    this.currentVersion = new SuperVersion(this.activeMemTable, this.immutableMemTables, this.objectEntries);
                    oldVersion.unref();
                } finally
                {
                    this.versionLock.writeLock().unlock();
                }
            } catch (Exception e)
            {
                logger.error("Error in flushMinioToDisk: ", e);
            } finally
            {
                for (ObjectEntry objectEntry : flushEntries)
                {
                    objectEntry.unref(); // unref in the end
                    String objectKey = this.schemaName + '/' + tableName + '/' + objectEntry.getId();
                    try
                    {
                        this.minio.delete(objectKey, false);
                    } catch (IOException e)
                    {
                        logger.error("fail to delete " + objectKey + " in minio", e);
                        throw new RuntimeException("fail to delete " + objectKey + " in minio", e);
                    }
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

    private void addRowBatchFromMinio(ObjectEntry objectEntry) throws RetinaException
    {
        try
        {
            String objectKey = this.schemaName + '/' + tableName + '/' + objectEntry.getId();
            PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(this.minio, objectKey);
            int length = (int) reader.getFileLength();
            byte[] data = new byte[length];
            reader.readFully(data, 0, length);
            this.writer.addRowBatch(VectorizedRowBatch.deserialize(data));
        } catch (IOException e)
        {
            logger.error("Failed to load rowBatch from minio", e);
            throw new RetinaException("Failed to load rowBatch from minio", e);
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
        this.flushExecutor.shutdown();

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

            // add minio object to writer
            for (ObjectEntry objectEntry : sv.getObjectEntries())
            {
                addRowBatchFromMinio(objectEntry);
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
            for (ObjectEntry objectEntry : sv.getObjectEntries())
            {
                objectEntry.unref();
                this.minio.delete(this.schemaName + '/' + this.tableName + '/' + objectEntry.getId(), false);
            }
        }
    }
}
