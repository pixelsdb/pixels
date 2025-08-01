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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;

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

    private final long tableId;

    // Column information is recorded to create rowBatch.
    private final TypeDescription schema;

    // Configuration information of PixelsWriter
    private final int memTableSize;
    private final long blockSize;
    private final short replication;
    private final EncodingLevel encodingLevel;
    private final boolean nullsPadding;
    private final int maxMemTableCount;  // threshold number of memTable to be dumped to file
    private final Path targetOrderedDirPath;
    private final Path targetCompactDirPath;
    private final Storage targetOrderedStorage;
    private final Storage targetCompactStorage;
    private final RowIdAllocator rowIdAllocator;
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
    private final MinioManager minioManager;
    private final List<ObjectEntry> objectEntries;

    // Current data view
    private volatile SuperVersion currentVersion;

    // Backend flush thread
    private final ExecutorService flushMinioExecutor;
    private final ScheduledExecutorService flushDiskExecutor;
    private ScheduledFuture<?> flushDiskFuture;

    // Lock for SuperVersion switch
    private final ReadWriteLock versionLock = new ReentrantReadWriteLock();

    private int currentMemTableCount;
    private final List<FileWriterManager> fileWriterManagers;
    private FileWriterManager currentFileWriterManager;
    private AtomicLong maxObjectKey;

    public PixelsWriterBuffer(long tableId, TypeDescription schema, Path targetOrderedDirPath,
                              Path targetCompactDirPath) throws RetinaException
    {
        this.tableId = tableId;
        this.schema = schema;

        ConfigFactory configFactory = ConfigFactory.Instance();
        this.memTableSize = Integer.parseInt(configFactory.getProperty("retina.buffer.memTable.size"));
        checkArgument(this.memTableSize % 64 == 0,"memTable size must be a multiple of 64.");
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
        this.maxMemTableCount = Integer.parseInt(configFactory.getProperty("retina.buffer.flush.count"));

        this.immutableMemTables = new ArrayList<>();
        this.objectEntries = new ArrayList<>();

        this.flushMinioExecutor = Executors.newSingleThreadExecutor();
        this.flushDiskExecutor = Executors.newSingleThreadScheduledExecutor();

        this.fileWriterManagers = new ArrayList<>();
        this.maxObjectKey = new AtomicLong(-1);

        // minio
        this.minioManager = MinioManager.Instance();

        this.currentFileWriterManager = new FileWriterManager(
                this.tableId, this.schema, this.targetOrderedDirPath,
                this.targetOrderedStorage, this.memTableSize, this.blockSize,
                this.replication, this.encodingLevel, this.nullsPadding,
                0, this.memTableSize * this.maxMemTableCount);

        this.activeMemTable = new MemTable(this.idCounter, schema, memTableSize,
                TypeDescription.Mode.NONE, this.currentFileWriterManager.getFileId(),
                0, this.memTableSize);
        this.idCounter++;
        this.currentMemTableCount = 1;

        // Initialization adds reference counts to all data
        this.currentVersion = new SuperVersion(activeMemTable, immutableMemTables, objectEntries);
        this.rowIdAllocator = new RowIdAllocator(tableId, 10); // TODO(AntiO2): Batch Size

        startFlushMinioToDiskScheduler();
    }

    /**
     * values is all column values, add values and timestamp into buffer
     *
     * @param values
     * @param timestamp
     * @return RowID
     */
    public long addRow(byte[][] values, long timestamp, IndexProto.RowLocation.Builder builder) throws RetinaException
    {
        int columnCount = this.schema.getChildren().size();
        checkArgument(values.length == columnCount,
                "Column values count does not match schema column count");

        boolean added = false;
        while (!added)
        {
            this.versionLock.readLock().lock();
            added = this.activeMemTable.add(values, timestamp);
            this.versionLock.readLock().unlock();
            if (!added)  // active memTable is full
            {
                switchMemTable();
            }
        }
        int rowOffset = (int) (currentMemTableCount * blockSize - blockSize + idCounter);
        builder.setFileId(currentFileWriterManager.getFileId())
                .setRgId(0)
                .setRgRowOffset(rowOffset);

        return rowIdAllocator.getRowId();
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

            if (this.currentMemTableCount >= this.maxMemTableCount)
            {
                this.currentMemTableCount = 0;
                this.currentFileWriterManager.setLastBlockId(this.activeMemTable.getId());
                this.fileWriterManagers.add(this.currentFileWriterManager);
                this.currentFileWriterManager = new FileWriterManager(
                        this.tableId, this.schema,
                        this.targetOrderedDirPath, this.targetOrderedStorage,
                        this.memTableSize, this.blockSize, this.replication,
                        this.encodingLevel, this.nullsPadding, this.idCounter,
                        this.memTableSize * this.maxMemTableCount);
            }

            /**
             * For activeMemTable, at initialization the reference count is 2 because of *this and superVersion
             * Here only currentVersion is destroyed, *this is still in use, so only one call to unref() is needed.
             */
            MemTable oldMemTable = this.activeMemTable;
            SuperVersion oldVersion = this.currentVersion;
            this.immutableMemTables.add(this.activeMemTable);
            this.activeMemTable = new MemTable(this.idCounter, this.schema,
                    this.memTableSize, TypeDescription.Mode.NONE,
                    this.currentFileWriterManager.getFileId(),
                    this.currentMemTableCount * this.memTableSize,
                    this.memTableSize);
            this.currentMemTableCount += 1;
            this.idCounter++;

            SuperVersion newVersion = new SuperVersion(this.activeMemTable, this.immutableMemTables, this.objectEntries);
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
                this.minioManager.write(this.tableId, id, flushMemTable.serialize());

                ObjectEntry objectEntry = new ObjectEntry(id, flushMemTable.getFileId(),
                        flushMemTable.getStartIndex(), flushMemTable.getLength());
                objectEntry.ref();

                this.maxObjectKey.updateAndGet(current -> Math.max(current, id));

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
        this.flushDiskFuture = this.flushDiskExecutor.scheduleWithFixedDelay(() -> {
            try
            {
                Iterator<FileWriterManager> iterator = this.fileWriterManagers.iterator();
                while (iterator.hasNext())
                {
                    FileWriterManager fileWriterManager = iterator.next();
                    if (fileWriterManager.getLastBlockId() <= this.maxObjectKey.get())
                    {
                        fileWriterManager.finish();
                        iterator.remove();

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
                            if (objectEntry.unref())
                            {
                                this.minioManager.delete(this.tableId, objectEntry.getId());
                            }
                        }
                    }
                }
            } catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    /**
     * collect resources
     *
     * @throws IOException
     */
    public void close() throws IOException
    {
        // First, shut down the flush process to prevent changes to the data view.
        this.flushMinioExecutor.shutdown();
        try
        {
            this.flushMinioExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e)
        {
            logger.error("Failed to shutdown flushMinioExecutor", e);
            Thread.currentThread().interrupt();
        }
        if (this.flushDiskFuture != null)
        {
            this.flushDiskFuture.cancel(false);
        }
        this.flushDiskExecutor.shutdown();
        try
        {
            this.flushDiskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e)
        {
            logger.error("Failed to shutdown flushDiskExecutor", e);
            Thread.currentThread().interrupt();
        }

        SuperVersion sv = getCurrentVersion();
        try
        {
            long maxObjectKey = this.maxObjectKey.get();

            // process current fileWriterManager
            this.currentFileWriterManager.setLastBlockId(maxObjectKey);
            this.currentFileWriterManager.addRowBatch(sv.getActiveMemTable().getRowBatch());
            long firstBlockId = this.currentFileWriterManager.getFirstBlockId();
            Iterator<MemTable> iterator = sv.getImmutableMemTables().iterator();
            while (iterator.hasNext())
            {
                MemTable immutableMemtable = iterator.next();
                if (immutableMemtable.getId() >= firstBlockId)
                {
                    this.currentFileWriterManager.addRowBatch(immutableMemtable.getRowBatch());
                    iterator.remove();
                }
            }
            this.currentFileWriterManager.finish();

            // process the remaining fileWriterManager
            for (FileWriterManager fileWriterManager : this.fileWriterManagers)
            {
                firstBlockId = fileWriterManager.getFirstBlockId();
                long lastBlockId = fileWriterManager.getLastBlockId();

                // all written to minio
                if (lastBlockId <= maxObjectKey)
                {
                    fileWriterManager.finish();
                } else
                {
                    // process elements in immutable memTable
                    iterator = sv.getImmutableMemTables().iterator();
                    while (iterator.hasNext())
                    {
                        MemTable immutableMemtable = iterator.next();
                        long id = immutableMemtable.getId();
                        if (id >= firstBlockId && id <= lastBlockId)
                        {
                            fileWriterManager.addRowBatch(immutableMemtable.getRowBatch());
                            iterator.remove();
                        }
                    }

                    // elements in minio will be processed in finish() later
                    fileWriterManager.setLastBlockId(maxObjectKey);
                    fileWriterManager.finish();
                }
            }
        } catch (Exception e)
        {
            logger.error("Error in close: ", e);
        } finally
        {
            sv.unref();
            currentVersion.unref();
            activeMemTable.unref();
            for (MemTable immutableMemTable: sv.getImmutableMemTables())
            {
                immutableMemTable.unref();
            }
            for (ObjectEntry objectEntry : sv.getObjectEntries())
            {
                objectEntry.unref();
                this.minioManager.delete(this.tableId, objectEntry.getId());
            }
        }
    }
}
