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

import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.IndexServiceProvider;
import io.pixelsdb.pixels.common.index.RowIdAllocator;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Data flows from the CDC into pixels, where it is first written to
 * the writer buffer and becomes visible externally. Each logical table
 * corresponds to a writer buffer, which contains only one memTable responsible
 * for writing data. Once the memTable is full, data is written to an immutable
 * memTable, and multiple immutable memTables can exist. Currently, to simplify
 * the distributed design, the immutable memTable is first written to the shared
 * storage, and then the data in shared storage is dumped to a disk file.
 *  -----        --------------------------------      ----------------      -----------
 * | CDC | ===> | memTable -> immutable memTable | -> | shared storage | -> | disk file |
 *  -----        --------------------------------      ----------------      -----------
 */
public class PixelsWriterBuffer
{
    private static final Logger logger = LogManager.getLogger(PixelsWriterBuffer.class);

    private final long tableId;

    // column information is recorded to create rowBatch
    private final TypeDescription schema;

    // configuration information of PixelsWriter
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
     * Allocate unique identifier for data (MemTable/MinioEntry).
     * There is no need to use atomic variables because
     * there are write locks in all concurrent situations.
     */
    private long idCounter = 0L;

    // active memTable
    private MemTable activeMemTable;

    // wait to refresh to shared storage
    private final List<MemTable> immutableMemTables;

    // object storage manager
    private final ObjectStorageManager objectStorageManager;
    private final List<ObjectEntry> objectEntries;

    // current data view
    private volatile SuperVersion currentVersion;

    // backend flush thread
    private final ExecutorService flushMinioExecutor;
    private final ScheduledExecutorService flushDiskExecutor;
    private ScheduledFuture<?> flushDiskFuture;

    // lock for SuperVersion switch
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
        checkArgument(this.memTableSize % 64 == 0,"MemTable size must be a multiple of 64.");
        this.targetOrderedDirPath = targetOrderedDirPath;
        this.targetCompactDirPath = targetCompactDirPath;
        try
        {
            this.targetOrderedStorage = StorageFactory.Instance().getStorage(targetOrderedDirPath.getUri());
            this.targetCompactStorage = StorageFactory.Instance().getStorage(targetCompactDirPath.getUri());
        } catch (Exception e)
        {
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

        this.objectStorageManager = ObjectStorageManager.Instance();

        this.currentFileWriterManager = new FileWriterManager(
                this.tableId, this.schema, this.targetOrderedDirPath,
                this.targetOrderedStorage, this.memTableSize, this.blockSize,
                this.replication, this.encodingLevel, this.nullsPadding,
                idCounter, this.memTableSize * this.maxMemTableCount);

        this.activeMemTable = new MemTable(this.idCounter, schema, memTableSize,
                TypeDescription.Mode.CREATE_INT_VECTOR_FOR_INT, this.currentFileWriterManager.getFileId(),
                0, this.memTableSize);
        this.idCounter++;
        this.currentMemTableCount = 1;

        // initialization adds reference counts to all data
        this.currentVersion = new SuperVersion(activeMemTable, immutableMemTables, objectEntries);
        this.rowIdAllocator = new RowIdAllocator(tableId, this.memTableSize, IndexServiceProvider.ServiceMode.local);

        startFlushMinioToDiskScheduler(Long.parseLong(configFactory.getProperty("retina.buffer.flush.interval")));
    }

    /**
     * Add all column values and timestamp into the buffer.
     *
     * @param values
     * @param timestamp
     * @param builder
     * @return the unique row identifier (rowId) allocated for the added row
     */
    public long addRow(byte[][] values, long timestamp, IndexProto.RowLocation.Builder builder) throws RetinaException
    {
        checkArgument(values.length == this.schema.getChildren().size(),
                "Column values count does not match schema column count.");

        MemTable currentMemTable = null;
        int rowOffset = -1;
        while (rowOffset < 0)
        {
            currentMemTable = this.activeMemTable;
            try
            {
                rowOffset = currentMemTable.add(values, timestamp);
            } catch (NullPointerException e)
            {
                continue;
            }

            // active memTable is full
            if (rowOffset < 0)
            {
                switchMemTable();
            }
        }
        int rgRowOffset = currentMemTable.getStartIndex() + rowOffset;
        if(rgRowOffset < 0)
        {
            throw new RetinaException("Expect rgRowOffset >= 0, get " + rgRowOffset);
        }
        builder.setFileId(activeMemTable.getFileId())
                .setRgId(0)
                .setRgRowOffset(rgRowOffset);
        try
        {
            return rowIdAllocator.getRowId();
        } catch (IndexException e)
        {
            throw new RetinaException("Fail to get rowId from rowIdAllocator", e);
        }
    }

    private void switchMemTable() throws RetinaException
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

            /*
             * For activeMemTable, at initialization the reference count is 2 because of *this and superVersion
             * Here only currentVersion is destroyed, *this is still in use, so only one call to unref() is needed.
             */
            MemTable oldMemTable = this.activeMemTable;
            SuperVersion oldVersion = this.currentVersion;
            this.immutableMemTables.add(this.activeMemTable);
            this.activeMemTable = new MemTable(this.idCounter, this.schema,
                    this.memTableSize, TypeDescription.Mode.CREATE_INT_VECTOR_FOR_INT,
                    this.currentFileWriterManager.getFileId(),
                    this.currentMemTableCount * this.memTableSize,
                    this.memTableSize);
            this.currentMemTableCount += 1;
            this.idCounter++;

            this.currentVersion = new SuperVersion(this.activeMemTable, this.immutableMemTables, this.objectEntries);
            oldVersion.unref();

            triggerFlushToMinio(oldMemTable);
        } catch (Exception e)
        {
            throw new RetinaException("Failed to switch memtable", e);
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
                this.objectStorageManager.write(this.tableId, id, flushMemTable.serialize());

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

                // unref in the end
                flushMemTable.unref();
            } catch (Exception e)
            {
                // TODO: Retry on failure.
                logger.error("Failed to flush memTable to shared storage, memTableId={}", flushMemTable.getId(), e);
            }
        });
    }

    /**
     * Get the current version.
     * Caller must call unref().
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
     * been written to MinIO. If it has been written, execute the file write
     * operation and delete the corresponding ObjectEntry in the unified view.
     */
    private void startFlushMinioToDiskScheduler(long intervalSeconds)
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
                        CompletableFuture<Void> finished = fileWriterManager.finish();
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

                        finished.get();
                        for (ObjectEntry objectEntry : toRemove)
                        {
                            if (objectEntry.unref())
                            {
                                this.objectStorageManager.delete(this.tableId, objectEntry.getId());
                            }
                        }
                    }
                }
            } catch (Exception e)
            {
                logger.error("Failed to flush data to disk", e);
            }
        }, 0, intervalSeconds, TimeUnit.SECONDS);
    }

    /**
     * Gracefully close the writer buffer, ensuring all in-memory data is persisted.
     */
    public void close() throws RetinaException
    {
        // First, shut down the flush process to prevent changes to the data view.
        this.flushMinioExecutor.shutdown();
        try
        {
            if (!this.flushMinioExecutor.awaitTermination(60, TimeUnit.SECONDS))
            {
                this.flushMinioExecutor.shutdownNow();
            }
        } catch (InterruptedException e)
        {
            this.flushMinioExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            throw new RetinaException("Close process was interrupted while waiting for flushMinioExecutor", e);
        }
        if (this.flushDiskFuture != null)
        {
            this.flushDiskFuture.cancel(false);
        }
        this.flushDiskExecutor.shutdown();
        try
        {
            if (!this.flushDiskExecutor.awaitTermination(60, TimeUnit.SECONDS))
            {
                this.flushDiskExecutor.shutdownNow();
            }
        } catch (InterruptedException e)
        {
            this.flushDiskExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            throw new RetinaException("Close process was interrupted while waiting for flushDiskExecutor", e);
        }

        SuperVersion sv = getCurrentVersion();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
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
            this.currentFileWriterManager.finish().get();

            // process the remaining fileWriterManager
            for (FileWriterManager fileWriterManager : this.fileWriterManagers)
            {
                firstBlockId = fileWriterManager.getFirstBlockId();
                long lastBlockId = fileWriterManager.getLastBlockId();

                // all written to minio
                if (lastBlockId <= maxObjectKey)
                {
                    futures.add(fileWriterManager.finish());
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
                    futures.add(fileWriterManager.finish());
                }
            }

            CompletableFuture<Void> all = CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0])
            );
            all.get(15, TimeUnit.SECONDS);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RetinaException("Data persistence was interrupted during close", e);
        } catch (Exception e)
        {
            throw new RetinaException("Failed to persist data during close operation. Data may be lost", e);
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
                this.objectStorageManager.delete(this.tableId, objectEntry.getId());
            }
        }
    }
}
