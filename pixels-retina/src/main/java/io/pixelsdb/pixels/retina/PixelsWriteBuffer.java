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
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.service.IndexServiceProvider;
import io.pixelsdb.pixels.common.index.RowIdAllocator;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

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
public class PixelsWriteBuffer
{
    private static final Logger logger = LogManager.getLogger(PixelsWriteBuffer.class);

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
     * Allocate unique identifier for data (MemTable/ObjectEntry).
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
    private final ExecutorService flushObjectExecutor;
    // Single-threaded by design: it serializes file publishing and FileWriterManager physical close initialization.
    private final ScheduledExecutorService flushFileExecutor;
    private ScheduledFuture<?> flushFileFuture;

    // lock for SuperVersion switch
    private final ReadWriteLock versionLock = new ReentrantReadWriteLock();

    private int currentMemTableCount;
    private final Queue<FileWriterManager> fileWriterManagers;
    private FileWriterManager currentFileWriterManager;
    private IngestFilePublisher ingestFilePublisher;

    /**
     * Issue #1254: Multi-threaded flush
     * Add `outOfOrderFlushedIds` to store flushed IDs, and `continuousFlushedId`
     * to represent the maximum value of consecutively flushed IDs.
     */
    private AtomicLong continuousFlushedId;
    private final PriorityQueue<Long> outOfOrderFlushedIds;
    private final Object flushLock = new Object();
    private final Object rowLock = new Object();

    private String retinaHostName;
    private SinglePointIndex index;
    private final int virtualNodeId;
    private final IndexOption indexOption;

    public PixelsWriteBuffer(long tableId, TypeDescription schema, Path targetOrderedDirPath,
                             Path targetCompactDirPath, String retinaHostName, int virtualNode) throws RetinaException
    {
        this.tableId = tableId;
        this.schema = schema;
        this.virtualNodeId = virtualNode;
        this.indexOption = IndexOption.builder()
                .vNodeId(virtualNodeId)
                .build();

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

        this.flushObjectExecutor = Executors.newFixedThreadPool(Integer.parseInt(configFactory.getProperty("retina.buffer.object.flush.threads")));
        // Keep file publishing serialized: physical close, index flush, metadata publish, and cleanup are ordered per stream.
        this.flushFileExecutor = Executors.newSingleThreadScheduledExecutor();

        this.fileWriterManagers = new ConcurrentLinkedQueue<>();
        this.continuousFlushedId = new AtomicLong(-1);
        this.outOfOrderFlushedIds = new PriorityQueue<>();

        this.retinaHostName = retinaHostName;
        this.objectStorageManager = ObjectStorageManager.Instance();
        this.objectStorageManager.setIdPrefix(retinaHostName + "_");

        this.currentFileWriterManager = new FileWriterManager(
                this.tableId, this.schema, this.targetOrderedDirPath,
                this.targetOrderedStorage, this.memTableSize, this.blockSize,
                this.replication, this.encodingLevel, this.nullsPadding,
                idCounter, this.memTableSize * this.maxMemTableCount, retinaHostName, virtualNodeId);
        this.ingestFilePublisher = new IngestFilePublisher(this.currentFileWriterManager.getFirstBlockId());

        this.activeMemTable = new MemTable(this.idCounter, schema, memTableSize,
                TypeDescription.Mode.CREATE_INT_VECTOR_FOR_INT, this.currentFileWriterManager.getFileId(),
                0, this.memTableSize);
        this.idCounter++;
        this.currentMemTableCount = 1;

        // initialization adds reference counts to all data
        this.currentVersion = new SuperVersion(activeMemTable, immutableMemTables, objectEntries);
        this.rowIdAllocator = new RowIdAllocator(tableId, this.memTableSize, IndexServiceProvider.ServiceMode.local);

        startFlushObjectToFileScheduler(Long.parseLong(configFactory.getProperty("retina.buffer.flush.interval")));
    }

    /**
     * Append a row to the active memTable atomically. On return the row is
     * query-visible and {@code builder} is populated with its
     * {@link IndexProto.RowLocation} for downstream MainIndex / primary index
     * writes. If those writes fail, the caller MUST compensate by writing an
     * RGVisibility delete on that RowLocation; do not try to rewind the append.
     *
     * @param values the column values of the row.
     * @param timestamp the commit timestamp of the row.
     * @param builder the builder of the row location, populated on return.
     * @return the allocated rowId.
     * @throws RetinaException if the buffer is fail-closed or rowId allocation fails.
     */
    public long addRow(byte[][] values, long timestamp, IndexProto.RowLocation.Builder builder) throws RetinaException
    {
        checkArgument(values.length == this.schema.getChildren().size(),
                "Column values count does not match schema column count.");

        MemTable currentMemTable = null;
        int rowOffset = -1;
        long rowId = -1;
        while (rowOffset < 0)
        {
            try
            {
                synchronized (rowLock)
                {
                    currentMemTable = this.activeMemTable;
                    FileWriterManager appendFileWriterManager = this.currentFileWriterManager;
                    // Keep row offsets and row IDs aligned for index flush.
                    rowOffset = currentMemTable.add(values, timestamp);
                    if (rowOffset >= 0)
                    {
                        rowId = rowIdAllocator.getRowId();
                        appendFileWriterManager.includeRowId(rowId);
                    }
                }
            } catch (NullPointerException e)
            {
                continue;
            } catch (IndexException e)
            {
                throw new RetinaException("Fail to get rowId from rowIdAllocator", e);
            }

            // active memTable is full
            if (rowOffset < 0)
            {
                switchMemTable();
            }
        }
        int rgRowOffset = currentMemTable.getStartIndex() + rowOffset;
        if (rgRowOffset < 0)
        {
            throw new RetinaException("Expect rgRowOffset >= 0, get " + rgRowOffset);
        }
        builder.setFileId(currentMemTable.getFileId())
                .setRgId(0)
                .setRgRowOffset(rgRowOffset);
        return rowId;
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
            retireActiveMemTableLocked();
        } catch (Exception e)
        {
            throw new RetinaException("Failed to switch memtable", e);
        } finally
        {
            this.versionLock.writeLock().unlock();
        }
    }

    // Caller must hold versionLock.writeLock().
    private void retireActiveMemTableLocked() throws RetinaException
    {
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
                    this.memTableSize * this.maxMemTableCount, this.retinaHostName, virtualNodeId);
        }
            
        /*
         * For activeMemTable, at initialization the reference count is 2 because of *this and currentVersion
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

        triggerFlushToObject(oldMemTable);
    }

    private void triggerFlushToObject(MemTable flushMemTable)
    {
        flushObjectExecutor.submit(() -> {
            try
            {
                // put into object storage
                long id = flushMemTable.getId();
                this.objectStorageManager.write(this.tableId, virtualNodeId, id, flushMemTable.serialize());

                ObjectEntry objectEntry = new ObjectEntry(id, flushMemTable.getFileId(),
                        flushMemTable.getStartIndex(), flushMemTable.getSize());
                objectEntry.ref();

                // update watermark
                synchronized (flushLock)
                {
                    long nextId = continuousFlushedId.get() + 1;
                    if (id == nextId)
                    {
                        continuousFlushedId.incrementAndGet();
                        while (!outOfOrderFlushedIds.isEmpty() && outOfOrderFlushedIds.peek() == continuousFlushedId.get() + 1)
                        {
                            outOfOrderFlushedIds.poll();
                            continuousFlushedId.incrementAndGet();
                        }
                    } else
                    {
                        outOfOrderFlushedIds.add(id);
                    }
                }

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

    private List<FileWriterManager> publishFinishedFile(FileWriterManager fileWriterManager) throws RetinaException
    {
        try
        {
            fileWriterManager.finish();

            if (!fileWriterManager.isIndexFlushed())
            {
                if (this.index == null)
                {
                    this.index = MetadataService.Instance().getPrimaryIndex(tableId);
                    if (this.index == null)
                    {
                        throw new RetinaException("Primary index not found for table " + tableId);
                    }
                }

                boolean flushed = IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.local)
                        .flushIndexEntriesOfFile(
                                tableId, index.getId(), fileWriterManager.getFileId(), true, indexOption);
                if (!flushed)
                {
                    throw new RetinaException("Failed to flush main index for ingest file "
                            + fileWriterManager.getFileId());
                }
                fileWriterManager.markIndexFlushed();
            }
        } catch (IndexException e)
        {
            throw new RetinaException("Failed to flush main index for ingest file "
                    + fileWriterManager.getFileId(), e);
        } catch (MetadataException e)
        {
            throw new RetinaException("Failed to load primary index for table " + tableId, e);
        }
        return this.ingestFilePublisher.admitReady(fileWriterManager, this::publishPreparedFile);
    }

    private void publishPreparedFile(FileWriterManager fileWriterManager) throws RetinaException
    {
        try
        {
            if (!fileWriterManager.isPhysicalClosed())
            {
                throw new RetinaException("Cannot publish ingest file before physical close: fileId="
                        + fileWriterManager.getFileId());
            }
            if (!fileWriterManager.isIndexFlushed())
            {
                throw new RetinaException("Cannot publish ingest file before main index flush: fileId="
                        + fileWriterManager.getFileId());
            }
            if (!fileWriterManager.hasRowIds())
            {
                throw new RetinaException("Cannot publish ingest file without row-id hull: fileId="
                        + fileWriterManager.getFileId());
            }
            File regularFile = fileWriterManager.getFileSnapshot();
            regularFile.setType(File.Type.REGULAR);
            if (!MetadataService.Instance().updateFile(regularFile))
            {
                throw new RetinaException("Failed to publish ingest file "
                        + fileWriterManager.getFileId() + " as REGULAR");
            }
            RetinaResourceManager.Instance().registerIngestFileMetadata(
                    fileWriterManager.getFileId(), tableId, fileWriterManager.getVirtualNodeId(),
                    fileWriterManager.getFirstBlockId());
        } catch (MetadataException e)
        {
            throw new RetinaException("Failed to publish ingest file "
                    + fileWriterManager.getFileId() + " as REGULAR", e);
        }
    }

    /**
     * Determine whether the last data block managed by fileWriterManager has
     * been written to Object. If it has been written, execute the file write
     * operation and delete the corresponding ObjectEntry in the unified view.
     */
    private void startFlushObjectToFileScheduler(long intervalSeconds)
    {
        this.flushFileFuture = this.flushFileExecutor.scheduleWithFixedDelay(() -> {
            try
            {
                Iterator<FileWriterManager> iterator = this.fileWriterManagers.iterator();
                while (iterator.hasNext())
                {
                    FileWriterManager fileWriterManager = iterator.next();
                    if (fileWriterManager.getLastBlockId() > this.continuousFlushedId.get())
                    {
                        break;
                    }
                    List<FileWriterManager> publishedFiles = publishFinishedFile(fileWriterManager);
                    for (FileWriterManager publishedFile : publishedFiles)
                    {
                        this.fileWriterManagers.remove(publishedFile);
                        cleanupPublishedObjects(publishedFile.getFirstBlockId(), publishedFile.getLastBlockId());
                    }
                }
            } catch (Exception e)
            {
                logger.error("Failed to flush data to disk", e);
            }
        }, 0, intervalSeconds, TimeUnit.SECONDS);
    }

    private void cleanupPublishedObjects(long firstBlockId, long lastBlockId) throws RetinaException
    {
        if (lastBlockId < firstBlockId)
        {
            return;
        }

        List<ObjectEntry> toRemove;
        this.versionLock.writeLock().lock();
        try
        {
            toRemove = this.objectEntries.stream()
                    .filter(objectEntry -> objectEntry.getId() >= firstBlockId && objectEntry.getId() <= lastBlockId)
                    .collect(Collectors.toList());
            this.objectEntries.removeAll(toRemove);

            SuperVersion oldVersion = this.currentVersion;
            this.currentVersion = new SuperVersion(
                    this.activeMemTable, this.immutableMemTables, this.objectEntries);
            oldVersion.unref();
        } finally
        {
            this.versionLock.writeLock().unlock();
        }

        for (ObjectEntry objectEntry : toRemove)
        {
            if (objectEntry.unref())
            {
                this.objectStorageManager.delete(this.tableId, virtualNodeId, objectEntry.getId());
            }
        }
    }

    public void close() throws RetinaException
    {
        // The caller (RetinaServer / RetinaResourceManager shutdown path) is
        // responsible for quiescing append traffic before invoking close().
        // There is no buffer-internal "append-to-publish" window to drain.
        // Stop scheduled publishing before the driver thread publishes leftovers.
        if (this.flushFileFuture != null)
        {
            this.flushFileFuture.cancel(false);
        }
        this.flushFileExecutor.shutdown();
        try
        {
            if (!this.flushFileExecutor.awaitTermination(60, TimeUnit.SECONDS))
            {
                logger.warn("Close timed out waiting for flushFileExecutor to drain; proceeding");
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RetinaException("Close process was interrupted while waiting for flushFileExecutor", e);
        }

        // Retire non-empty active data so file close only replays ObjectEntry bytes.
        this.versionLock.writeLock().lock();
        try
        {
            if (!this.activeMemTable.isEmpty())
            {
                retireActiveMemTableLocked();
            }
        }
        finally
        {
            this.versionLock.writeLock().unlock();
        }

        // Let submitted object flushes finish; never interrupt in-flight uploads.
        this.flushObjectExecutor.shutdown();
        try
        {
            if (!this.flushObjectExecutor.awaitTermination(60, TimeUnit.SECONDS))
            {
                logger.warn("Close timed out waiting for flushObjectExecutor to drain; proceeding");
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RetinaException("Close process was interrupted while waiting for flushObjectExecutor", e);
        }

        // Publish files with rows; discard an empty current ingest file.
        if (this.currentFileWriterManager != null)
        {
            if (this.currentFileWriterManager.hasRowIds())
            {
                this.currentFileWriterManager.setLastBlockId(this.continuousFlushedId.get());
                this.fileWriterManagers.add(this.currentFileWriterManager);
            }
            else
            {
                FileWriterManager zeroDataFwm = this.currentFileWriterManager;
                String filePath = this.targetOrderedDirPath.getUri() + "/"
                        + zeroDataFwm.getFileName();
                try
                {
                    if (this.targetOrderedStorage.exists(filePath))
                    {
                        this.targetOrderedStorage.delete(filePath, false);
                    }
                }
                catch (IOException e)
                {
                    logger.warn("Close failed to delete half-written bytes of empty FileWriterManager fileId={}, path={}; continuing",
                            zeroDataFwm.getFileId(), filePath, e);
                }
                try
                {
                    zeroDataFwm.discard();
                }
                catch (RetinaException e)
                {
                    logger.warn("Close failed to discard empty current FileWriterManager fileId={}; continuing",
                            zeroDataFwm.getFileId(), e);
                }
            }
            this.currentFileWriterManager = null;
        }

        SuperVersion sv = getCurrentVersion();
        try
        {
            for (FileWriterManager fwm : new ArrayList<>(this.fileWriterManagers))
            {
                List<FileWriterManager> published = publishFinishedFile(fwm);
                for (FileWriterManager publishedFile : published)
                {
                    this.fileWriterManagers.remove(publishedFile);
                    cleanupPublishedObjects(publishedFile.getFirstBlockId(), publishedFile.getLastBlockId());
                }
            }
        }
        catch (Exception e)
        {
            throw new RetinaException("Failed to publish ingest files during close", e);
        }
        finally
        {
            sv.unref();
        }
    }
}
