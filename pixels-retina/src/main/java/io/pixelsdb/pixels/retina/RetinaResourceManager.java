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

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.common.utils.CheckpointFileIO;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.NetUtils;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Use the singleton pattern to manage data resources in the retina service.
 */
public class RetinaResourceManager
{
    private static final Logger logger = LogManager.getLogger(RetinaResourceManager.class);
    private final MetadataService metadataService;
    private final Map<String, RGVisibility> rgVisibilityMap;
    private final Map<String, Map<Integer, PixelsWriteBuffer>> pixelsWriteBufferMap;
    private String retinaHostName;

    // GC related fields
    private final ScheduledExecutorService gcExecutor;
    private final boolean storageGcEnabled;
    private final StorageGarbageCollector storageGarbageCollector;

    // Checkpoint related fields
    private final ExecutorService checkpointExecutor;
    private final Map<Long, String> offloadedCheckpoints;
    private final Map<Long, CompletableFuture<Void>> checkpointFutures;
    private final String checkpointDir;
    private volatile long latestGcTimestamp = -1;
    private final int totalVirtualNodeNum;

    private final Map<Long, AtomicInteger> checkpointRefCounts;

    // Dual-write: oldFileId → result AND newFileId → result in a single map.
    // Direction is distinguished by checking fileId == result.newFileId.
    private final Map<Long, StorageGarbageCollector.RewriteResult> dualWriteLookup = new HashMap<>();
    private final ReadWriteLock redirectionLock = new ReentrantReadWriteLock();
    private volatile boolean isDualWriteActive = false;

    // Delayed cleanup queue for old files retired after atomic swap.
    private final ConcurrentLinkedQueue<RetiredFile> retiredFiles = new ConcurrentLinkedQueue<>();

    /**
     * Metadata for an old file that has been atomically swapped out and awaits
     * delayed physical deletion after a configurable wall-clock grace period.
     */
    static final class RetiredFile
    {
        final long fileId;
        final int rgCount;
        final String filePath;
        /** Epoch millis deadline; the file is eligible for deletion after this time. */
        final long retireTimestamp;
        final List<Long> oldRowIds;

        RetiredFile(long fileId, int rgCount, String filePath, long retireTimestamp, List<Long> oldRowIds)
        {
            this.fileId = fileId;
            this.rgCount = rgCount;
            this.filePath = filePath;
            this.retireTimestamp = retireTimestamp;
            this.oldRowIds = oldRowIds;
        }
    }

    private enum CheckpointType
    {
        GC,
        OFFLOAD
    }

    private RetinaResourceManager()
    {
        this.metadataService = MetadataService.Instance();
        this.rgVisibilityMap = new ConcurrentHashMap<>();
        this.pixelsWriteBufferMap = new ConcurrentHashMap<>();
        this.offloadedCheckpoints = new ConcurrentHashMap<>();
        this.checkpointFutures = new ConcurrentHashMap<>();

        ConfigFactory config = ConfigFactory.Instance();

        this.checkpointRefCounts = new ConcurrentHashMap<>();
        this.checkpointDir = config.getProperty("retina.checkpoint.dir");

        int cpThreads = Integer.parseInt(config.getProperty("retina.checkpoint.threads"));
        this.checkpointExecutor = Executors.newFixedThreadPool(cpThreads, r -> {
            Thread t = new Thread(r, "retina-checkpoint-thread");
            t.setDaemon(true);
            return t;
        });

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "retina-gc-thread");
            t.setDaemon(true);
            return t;
        });
        try
        {
            long interval = Long.parseLong(config.getProperty("retina.gc.interval"));
            if (interval > 0)
            {
                executor.scheduleAtFixedRate(
                        this::runGC,
                        interval,
                        interval,
                        TimeUnit.SECONDS
                );
            }
        } catch (Exception e)
        {
            logger.error("Failed to start retina background gc", e);
        }
        this.gcExecutor = executor;
        totalVirtualNodeNum = Integer.parseInt(ConfigFactory.Instance().getProperty("node.virtual.num"));
        this.retinaHostName = NetUtils.getLocalHostName();

        boolean gcEnabled = false;
        StorageGarbageCollector gc = null;
        try
        {
            gcEnabled = Boolean.parseBoolean(config.getProperty("retina.storage.gc.enabled"));
            if (gcEnabled)
            {
                double threshold = Double.parseDouble(config.getProperty("retina.storage.gc.threshold"));
                long targetFileSize = Long.parseLong(config.getProperty("retina.storage.gc.target.file.size"));
                int maxFilesPerGroup = Integer.parseInt(config.getProperty("retina.storage.gc.max.files.per.group"));
                int maxGroups = Integer.parseInt(config.getProperty("retina.storage.gc.max.file.groups.per.run"));
                int rowGroupSize = Integer.parseInt(config.getProperty("row.group.size"));
                EncodingLevel encodingLevel = EncodingLevel.from(
                        Integer.parseInt(config.getProperty("retina.storage.gc.encoding.level")));
                long retireDelayMs = (long) (Double.parseDouble(config.getProperty("retina.storage.gc.file.retire.delay.hours")) * 3_600_000L);
                gc = new StorageGarbageCollector(this, this.metadataService,
                        threshold, targetFileSize, maxFilesPerGroup, maxGroups,
                        rowGroupSize, encodingLevel, retireDelayMs);
                logger.info("Storage GC enabled (threshold={}, targetFileSize={}, maxFilesPerGroup={}, maxGroups={})",
                        threshold, targetFileSize, maxFilesPerGroup, maxGroups);
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to initialise StorageGarbageCollector, Storage GC will be disabled", e);
            gcEnabled = false;
            gc = null;
        }
        this.storageGcEnabled = gcEnabled;
        this.storageGarbageCollector = gc;
    }

    private static final class InstanceHolder
    {
        private static final RetinaResourceManager instance = new RetinaResourceManager();
    }

    public static RetinaResourceManager Instance()
    {
        return InstanceHolder.instance;
    }

    public void addVisibility(long fileId, int rgId, int recordNum, long timestamp,
                              long[] bitmap, boolean overwrite)
    {
        String rgKey = RetinaUtils.buildRgKey(fileId, rgId);
        if (overwrite)
        {
            rgVisibilityMap.put(rgKey, new RGVisibility(recordNum, timestamp, bitmap));
        }
        else
        {
            rgVisibilityMap.computeIfAbsent(rgKey, k -> new RGVisibility(recordNum, timestamp, bitmap));
        }
    }

    public void addVisibility(String filePath) throws RetinaException
    {
        try
        {
            Storage storage = StorageFactory.Instance().getStorage(filePath);
            try (PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(storage, filePath))
            {
                long fileLen = fsReader.getFileLength();
                fsReader.seek(fileLen - Long.BYTES);
                long fileTailOffset = fsReader.readLong(ByteOrder.BIG_ENDIAN);
                int fileTailLength = (int) (fileLen - fileTailOffset - Long.BYTES);
                fsReader.seek(fileTailOffset);
                ByteBuffer fileTailBuffer = fsReader.readFully(fileTailLength);
                PixelsProto.FileTail fileTail = PixelsProto.FileTail.parseFrom(fileTailBuffer);
                PixelsProto.Footer footer = fileTail.getFooter();
                // TODO: fileId can be obtained directly through File.getId() method.
                long fileId = this.metadataService.getFileId(filePath);
                for (int rgId = 0; rgId < footer.getRowGroupInfosCount(); rgId++)
                {
                    int recordNum = footer.getRowGroupInfos(rgId).getNumberOfRows();
                    addVisibility(fileId, rgId, recordNum, 0L, null, false);
                }
            }
        } catch (Exception e)
        {
            throw new RetinaException("Failed to add visibility for file " +  filePath, e);
        }
    }

    public long[] queryVisibility(long fileId, int rgId, long timestamp, long transId) throws RetinaException
    {
        // read from memory
        RGVisibility rgVisibility = checkRGVisibility(fileId, rgId);
        long[] visibilityBitmap = rgVisibility.getVisibilityBitmap(timestamp);
        if (visibilityBitmap == null)
        {
            throw new RetinaException(String.format("Failed to get visibility for fileId: %d, rgId: %d", fileId, rgId));
        }
        return visibilityBitmap;
    }


    public long[] queryVisibility(long fileId, int rgId, long timestamp) throws RetinaException
    {
        return queryVisibility(fileId, rgId, timestamp, -1);
    }

    /**
     * Long-running queries register an "Offload" status and ensure that
     * the required visibility checkpoint is correctly created and manages.
     * For long-running transactions, newly written data is not required.
     * Therefore, even if checkpoints are created under the same timestamp
     * and only one copy is retained, this has virtually no impact on queries.
     *
     * @param timestamp
     * @throws RetinaException
     */
    public void registerOffload(long timestamp) throws RetinaException
    {
        AtomicInteger refCount = checkpointRefCounts.computeIfAbsent(timestamp, k -> new AtomicInteger(0));
        CompletableFuture<Void> future;

        synchronized (refCount)
        {
            refCount.incrementAndGet();

            // If checkpoint already exists and is fully committed, just return
            if (offloadedCheckpoints.containsKey(timestamp))
            {
                logger.info("Registered offload for Timestamp: {} (already exists)", timestamp);
                return;
            }

            // Check if there is an existing future
            future = checkpointFutures.get(timestamp);
            if (future != null && future.isCompletedExceptionally())
            {
                // If previous attempt failed, remove it so we can retry
                checkpointFutures.remove(timestamp, future);
                future = null;
            }

            if (future == null)
            {
                future = checkpointFutures.computeIfAbsent(timestamp, k -> {
                    try
                    {
                        return createCheckpoint(timestamp, CheckpointType.OFFLOAD);
                    } catch (RetinaException e)
                    {
                        throw new CompletionException(e);
                    }
                });
            }
        }

        try
        {
            future.join();
            logger.info("Registered offload for Timestamp: {}", timestamp);
        } catch (Exception e)
        {
            synchronized (refCount)
            {
                refCount.decrementAndGet();
                // We don't remove from checkpointFutures here anymore, 
                // because it's handled above in the synchronized block for retries
                // or let the next caller handle it.
            }
            throw new RetinaException("Failed to create checkpoint for timestamp: " + timestamp, e);
        }
    }

    public void unregisterOffload(long timestamp)
    {
        AtomicInteger refCount = checkpointRefCounts.get(timestamp);
        if (refCount != null)
        {
            synchronized (refCount)
            {
                int remaining = refCount.decrementAndGet();
                if (remaining <= 0)
                {
                    offloadedCheckpoints.remove(timestamp);
                    checkpointFutures.remove(timestamp);
                    if (refCount.get() > 0)
                    {
                        logger.info("Checkpoint resurrection detected, skipping deletion. TS: {}", timestamp);
                        return;
                    }
                    removeCheckpointFile(timestamp, CheckpointType.OFFLOAD);
                    checkpointRefCounts.remove(timestamp);
                    logger.info("Offload checkpoint for timestamp {} removed.", timestamp);
                }
            }
        }
    }

    private CompletableFuture<Void> createCheckpoint(long timestamp, CheckpointType type) throws RetinaException
    {
        return createCheckpoint(timestamp, type, null);
    }

    private CompletableFuture<Void> createCheckpoint(
            long timestamp, CheckpointType type, Map<String, long[]> precomputedBitmaps) throws RetinaException
    {
        String prefix = (type == CheckpointType.GC) ? RetinaUtils.CHECKPOINT_PREFIX_GC : RetinaUtils.CHECKPOINT_PREFIX_OFFLOAD;
        String filePath = RetinaUtils.buildCheckpointPath(checkpointDir, prefix, retinaHostName, timestamp);

        // 1. Capture current entries to ensure we process a consistent set of RGs
        List<Map.Entry<String, RGVisibility>> entries = new ArrayList<>(this.rgVisibilityMap.entrySet());
        int totalRgs = entries.size();
        logger.info("Starting {} checkpoint for {} RGs at timestamp {}", type, totalRgs, timestamp);

        // 2. Use a BlockingQueue for producer-consumer pattern
        BlockingQueue<CheckpointFileIO.CheckpointEntry> queue = new LinkedBlockingQueue<>(1024);

        // 3. Start producer tasks to fetch bitmaps
        for (Map.Entry<String, RGVisibility> entry : entries)
        {
            checkpointExecutor.submit(() -> {
                try
                {
                    String key = entry.getKey();
                    long fileId = RetinaUtils.parseFileIdFromRgKey(key);
                    int rgId = RetinaUtils.parseRgIdFromRgKey(key);
                    RGVisibility rgVisibility = entry.getValue();
                    long[] bitmap;
                    if (precomputedBitmaps != null && precomputedBitmaps.containsKey(key))
                    {
                        bitmap = precomputedBitmaps.get(key);
                    } else
                    {
                        bitmap = rgVisibility.getVisibilityBitmap(timestamp);
                    }
                    queue.put(new CheckpointFileIO.CheckpointEntry(fileId, rgId, (int) rgVisibility.getRecordNum(), bitmap));
                } catch (Exception e)
                {
                    logger.error("Failed to fetch visibility bitmap for checkpoint", e);
                }
            });
        }

        // 4. Async Write: perform IO in background thread (Consumer).
        // Use commonPool to avoid deadlocks with checkpointExecutor.
        // Concurrency safety: for OFFLOAD type, registerOffload() guarantees at most
        // one future per timestamp via synchronized(refCount) + checkpointFutures.computeIfAbsent.
        // For GC type, runGC() is single-threaded. No file-level locking is needed here.
        return CompletableFuture.runAsync(() -> {
            long startWrite = System.currentTimeMillis();
            try
            {
                CheckpointFileIO.writeCheckpoint(filePath, totalRgs, queue);
                long endWrite = System.currentTimeMillis();
                logger.info("Writing {} checkpoint file to {} took {} ms", type, filePath, (endWrite - startWrite));

                if (type == CheckpointType.OFFLOAD)
                {
                    offloadedCheckpoints.put(timestamp, filePath);
                }
            } catch (Exception e)
            {
                logger.error("Failed to commit {} checkpoint file for timestamp: {}", type, timestamp, e);
                try
                {
                    StorageFactory.Instance().getStorage(filePath).delete(filePath, false);
                } catch (IOException ignored)
                {
                }
                throw new CompletionException(e);
            }
        });
    }

    /**
     * Writes a checkpoint from pre-built {@link CheckpointFileIO.CheckpointEntry} objects,
     * bypassing the {@code rgVisibilityMap} traversal and per-entry thread-pool submission
     * that the other {@code createCheckpoint} overload performs.
     *
     * <p>This is used by {@link #runGC()} when the entries have already been constructed
     * during the Memory GC single-pass, avoiding a redundant second traversal of
     * {@code rgVisibilityMap}.
     */
    private CompletableFuture<Void> createCheckpointDirect(
            long timestamp, CheckpointType type,
            List<CheckpointFileIO.CheckpointEntry> preBuiltEntries) throws RetinaException
    {
        String prefix = (type == CheckpointType.GC) ? RetinaUtils.CHECKPOINT_PREFIX_GC : RetinaUtils.CHECKPOINT_PREFIX_OFFLOAD;
        String filePath = RetinaUtils.buildCheckpointPath(checkpointDir, prefix, retinaHostName, timestamp);

        int totalRgs = preBuiltEntries.size();
        logger.info("Starting {} checkpoint (direct) for {} RGs at timestamp {}", type, totalRgs, timestamp);

        BlockingQueue<CheckpointFileIO.CheckpointEntry> queue = new LinkedBlockingQueue<>(1024);

        // Feed pre-built entries into the queue via the checkpoint executor so that the
        // producer-consumer pattern with the writer thread is preserved (the queue has a
        // bounded capacity of 1024, so this may block and must not run on the caller thread).
        checkpointExecutor.submit(() -> {
            try
            {
                for (CheckpointFileIO.CheckpointEntry entry : preBuiltEntries)
                {
                    queue.put(entry);
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while feeding pre-built checkpoint entries", e);
            }
        });

        return CompletableFuture.runAsync(() -> {
            try
            {
                CheckpointFileIO.writeCheckpoint(filePath, totalRgs, queue);

                if (type == CheckpointType.OFFLOAD)
                {
                    offloadedCheckpoints.put(timestamp, filePath);
                }
            }
            catch (Exception e)
            {
                logger.error("Failed to commit {} checkpoint file for timestamp: {}", type, timestamp, e);
                try
                {
                    StorageFactory.Instance().getStorage(filePath).delete(filePath, false);
                }
                catch (IOException ignored)
                {
                }
                throw new CompletionException(e);
            }
        });
    }

    private void removeCheckpointFile(long timestamp, CheckpointType type)
    {
        String prefix = (type == CheckpointType.GC) ? RetinaUtils.CHECKPOINT_PREFIX_GC : RetinaUtils.CHECKPOINT_PREFIX_OFFLOAD;
        String path = RetinaUtils.buildCheckpointPath(checkpointDir, prefix, retinaHostName, timestamp);

        try
        {
            StorageFactory.Instance().getStorage(path).delete(path, false);
        } catch (IOException e)
        {
            logger.warn("Failed to delete checkpoint file", e);
        }
    }

    public void reclaimVisibility(long fileId, int rgId, long timestamp) throws RetinaException
    {
        String retinaKey = RetinaUtils.buildRgKey(fileId, rgId);
        RGVisibility rgVisibility = this.rgVisibilityMap.remove(retinaKey);
        if (rgVisibility != null)
        {
            rgVisibility.close();
        }
    }

    /**
     * Enqueues an old file for delayed cleanup after a configurable wall-clock
     * grace period has elapsed.
     */
    public void scheduleRetiredFile(RetiredFile retiredFile)
    {
        retiredFiles.add(retiredFile);
    }

    /**
     * Processes the retired files queue: for each file whose wall-clock
     * {@code retireTimestamp} deadline has passed, removes its Visibility
     * entries and deletes the physical file.
     */
    public void processRetiredFiles()
    {
        long now = System.currentTimeMillis();
        retiredFiles.removeIf(rf ->
        {
            if (now <= rf.retireTimestamp)
            {
                return false;
            }
            for (int rgId = 0; rgId < rf.rgCount; rgId++)
            {
                try
                {
                    reclaimVisibility(rf.fileId, rgId, 0);
                }
                catch (Exception e)
                {
                    logger.warn("processRetiredFiles: failed to reclaim Visibility for fileId={}, rgId={}",
                            rf.fileId, rgId, e);
                }
            }
            // Old MainIndex entries for retired files are purged lazily by the
            // MainIndex implementation; no explicit cleanup is needed here.
            if (rf.filePath != null)
            {
                try
                {
                    Storage storage = StorageFactory.Instance().getStorage(rf.filePath);
                    if (storage.exists(rf.filePath))
                    {
                        storage.delete(rf.filePath, false);
                    }
                }
                catch (IOException e)
                {
                    logger.warn("processRetiredFiles: failed to delete physical file {}", rf.filePath, e);
                }
            }
            return true;
        });
    }

    public String getCheckpointPath(long timestamp)
    {
        return offloadedCheckpoints.get(timestamp);
    }

    public void deleteRecord(long fileId, int rgId, int rgRowOffset, long timestamp) throws RetinaException
    {
        checkRGVisibility(fileId, rgId).deleteRecord(rgRowOffset, timestamp);

        if (!isDualWriteActive)
        {
            return;
        }

        redirectionLock.readLock().lock();
        try
        {
            StorageGarbageCollector.RewriteResult result = dualWriteLookup.get(fileId);
            if (result == null)
            {
                return;
            }

            if (fileId == result.newFileId)
            {
                // Backward: new file delete → sync to each old file
                for (StorageGarbageCollector.BackwardInfo bwd : result.backwardInfos)
                {
                    int[] bwdMapping = bwd.backwardRgMappings.get(rgId);
                    if (bwdMapping != null && rgRowOffset < bwdMapping.length && bwdMapping[rgRowOffset] >= 0)
                    {
                        int oldGlobal = bwdMapping[rgRowOffset];
                        int oldRgId = rgIdForGlobalRowOffset(oldGlobal, bwd.oldFileRgRowStart);
                        int oldRgOff = oldGlobal - bwd.oldFileRgRowStart[oldRgId];
                        checkRGVisibility(bwd.oldFileId, oldRgId).deleteRecord(oldRgOff, timestamp);
                    }
                }
            }
            else
            {
                // Forward: old file delete → sync to new file
                Map<Integer, int[]> fileMapping = result.forwardRgMappings.get(fileId);
                int[] fwdMapping = (fileMapping != null) ? fileMapping.get(rgId) : null;
                if (fwdMapping != null && rgRowOffset < fwdMapping.length && fwdMapping[rgRowOffset] >= 0)
                {
                    int newGlobal = fwdMapping[rgRowOffset];
                    int newRgId = rgIdForGlobalRowOffset(newGlobal, result.newFileRgRowStart);
                    int newRgOff = newGlobal - result.newFileRgRowStart[newRgId];
                    checkRGVisibility(result.newFileId, newRgId).deleteRecord(newRgOff, timestamp);
                }
            }
        }
        finally
        {
            redirectionLock.readLock().unlock();
        }
    }

    public void deleteRecord(IndexProto.RowLocation rowLocation, long timestamp) throws RetinaException
    {
        deleteRecord(rowLocation.getFileId(), rowLocation.getRgId(), rowLocation.getRgRowOffset(), timestamp);
    }

    /**
     * Registers dual-write redirection so that {@link #deleteRecord} propagates
     * deletes between old and new files.  The write lock acts as a barrier: all
     * prior deletes have completed before this returns, and all subsequent deletes
     * will see the new mappings.
     */
    void registerDualWrite(StorageGarbageCollector.RewriteResult result)
    {
        redirectionLock.writeLock().lock();
        try
        {
            for (Long oldFileId : result.forwardRgMappings.keySet())
            {
                dualWriteLookup.put(oldFileId, result);
            }
            dualWriteLookup.put(result.newFileId, result);
            isDualWriteActive = true;
        }
        finally
        {
            redirectionLock.writeLock().unlock();
        }
    }

    /**
     * Removes the dual-write redirection for the given rewrite result.
     */
    void unregisterDualWrite(StorageGarbageCollector.RewriteResult result)
    {
        redirectionLock.writeLock().lock();
        try
        {
            for (Long oldFileId : result.forwardRgMappings.keySet())
            {
                dualWriteLookup.remove(oldFileId);
            }
            dualWriteLookup.remove(result.newFileId);
            isDualWriteActive = !dualWriteLookup.isEmpty();
        }
        finally
        {
            redirectionLock.writeLock().unlock();
        }
    }

    long[] exportChainItemsAfter(long fileId, int rgId, long safeGcTs) throws RetinaException
    {
        return checkRGVisibility(fileId, rgId).exportChainItemsAfter(safeGcTs);
    }

    void importDeletionChain(long fileId, int rgId, long[] items) throws RetinaException
    {
        checkRGVisibility(fileId, rgId).importDeletionChain(items);
    }

    public void addWriteBuffer(String schemaName, String tableName) throws RetinaException
    {
        try
        {
            /*
             * Get ordered and compact dir path.
             * Already been validated when adding visibility.
             */
            Layout latestLayout = this.metadataService.getLatestLayout(schemaName, tableName);
            List<io.pixelsdb.pixels.common.metadata.domain.Path> orderedPaths = latestLayout.getOrderedPaths();
            List<io.pixelsdb.pixels.common.metadata.domain.Path> compactPaths = latestLayout.getCompactPaths();

            // get schema
            List<Column> columns = this.metadataService.getColumns(schemaName, tableName, false);
            List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
            List<String> columnTypes = columns.stream().map(Column::getType).collect(Collectors.toList());
            TypeDescription schema = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);

            String writeBufferKey = RetinaUtils.buildWriteBufferKey(schemaName, tableName);
            Map<Integer, PixelsWriteBuffer> nodeBuffers = pixelsWriteBufferMap.computeIfAbsent(
                    writeBufferKey, k -> new ConcurrentHashMap<>());

            for (int i = 0; i < totalVirtualNodeNum; i++)
            {
                PixelsWriteBuffer pixelsWriteBuffer = new PixelsWriteBuffer(latestLayout.getTableId(),
                        schema, orderedPaths.get(0), compactPaths.get(0), retinaHostName, i);
                nodeBuffers.put(i, pixelsWriteBuffer);
            }
        } catch (Exception e)
        {
            throw new RetinaException(String.format("Failed to add writer buffer for schema %s, table %s", schemaName, tableName), e);
        }
    }

    public IndexProto.PrimaryIndexEntry.Builder insertRecord(String schemaName, String tableName, byte[][] colValues, long timestamp, int vNodeId) throws RetinaException
    {
        IndexProto.PrimaryIndexEntry.Builder builder = IndexProto.PrimaryIndexEntry.newBuilder();
        PixelsWriteBuffer writeBuffer = checkPixelsWriteBuffer(schemaName, tableName, vNodeId);
        builder.setRowId(writeBuffer.addRow(colValues, timestamp, builder.getRowLocationBuilder()));
        return builder;
    }

    private RetinaProto.VisibilityBitmap getVisibilityBitmapSlice(long[] visibilityBitmap, long startIndex, int length) throws RetinaException
    {
        if (startIndex % 64 != 0 || length % 64 != 0)
        {
            throw new RetinaException("StartIndex and length must be multiple of 64");
        }
        if (length == 0)
        {
            return RetinaProto.VisibilityBitmap.newBuilder().build();
        }

        int startLongIndex = (int) (startIndex / 64);
        int endLongIndex = startLongIndex + (length / 64);

        if (visibilityBitmap == null || endLongIndex > visibilityBitmap.length)
        {
            throw new RetinaException("Cropping range exceeds the boundary");
        }

        long[] resultBitmap = Arrays.copyOfRange(visibilityBitmap, startLongIndex, endLongIndex);
        return RetinaProto.VisibilityBitmap.newBuilder()
                .addAllBitmap(Arrays.stream(resultBitmap).boxed().collect(Collectors.toList()))
                .build();
    }

    public RetinaProto.GetWriteBufferResponse.Builder getWriteBuffer(String schemaName, String tableName, long timestamp, int vNodeId) throws RetinaException
    {
        RetinaProto.GetWriteBufferResponse.Builder responseBuilder = RetinaProto.GetWriteBufferResponse.newBuilder();

        // get super version
        PixelsWriteBuffer writeBuffer = checkPixelsWriteBuffer(schemaName, tableName, vNodeId);
        SuperVersion superVersion = writeBuffer.getCurrentVersion();
        MemTable activeMemtable = superVersion.getActiveMemTable();
        List<MemTable> immutableMemTables = superVersion.getImmutableMemTables();
        List<ObjectEntry> objectEntries = superVersion.getObjectEntries();

        Set<Long> fileIds = new HashSet<>();

        // active memTable returns directly
        if (!activeMemtable.getRowBatch().isEmpty())
        {
            ByteString data = ByteString.copyFrom(activeMemtable.getRowBatch().serialize());
            responseBuilder.setData(data);

            fileIds.add(activeMemtable.getFileId());
        } else
        {
            responseBuilder.setData(ByteString.EMPTY);
        }

        // statistics on id and fileId
        List<Long> ids = new ArrayList<>();
        fileIds.add(activeMemtable.getFileId());
        for (MemTable immutableMemtable : immutableMemTables)
        {
            fileIds.add(immutableMemtable.getFileId());
            ids.add(immutableMemtable.getId());
        }
        for (ObjectEntry objectEntry : objectEntries)
        {
            fileIds.add(objectEntry.getFileId());
            ids.add(objectEntry.getId());
        }
        responseBuilder.addAllIds(ids);

        // get the visibility bitmap of fileIds
        Map<Long, long[]> fileIdToVisibility = new HashMap<>();
        for (Long fileId : fileIds)
        {
            long[] visibility = queryVisibility(fileId, 0, timestamp);
            fileIdToVisibility.put(fileId, visibility);
        }

        // only return the corresponding part of bitmap
        if (!activeMemtable.getRowBatch().isEmpty())
        {
            responseBuilder.addBitmaps(getVisibilityBitmapSlice(
                    fileIdToVisibility.get(activeMemtable.getFileId()),
                    activeMemtable.getStartIndex(), activeMemtable.getLength()));
        } else
        {
            responseBuilder.addBitmaps(RetinaProto.VisibilityBitmap.newBuilder());
        }
        for (MemTable immutableMemtable : immutableMemTables)
        {
            responseBuilder.addBitmaps(getVisibilityBitmapSlice(
                    fileIdToVisibility.get(immutableMemtable.getFileId()),
                    immutableMemtable.getStartIndex(), immutableMemtable.getLength()));
        }
        for (ObjectEntry objectEntry : objectEntries)
        {
            responseBuilder.addBitmaps(getVisibilityBitmapSlice(
                    fileIdToVisibility.get(objectEntry.getFileId()),
                    objectEntry.getStartIndex(), objectEntry.getLength()));
        }

        // unref super version
        superVersion.unref();

        return responseBuilder;
    }

    /**
     * Check if the retina exists for the given filePath and rgId.
     *
     * @param fileId the file id.
     * @param rgId the row group id.
     * @throws RetinaException if the retina does not exist.
     */
    private RGVisibility checkRGVisibility(long fileId, int rgId) throws RetinaException
    {
        String retinaKey = RetinaUtils.buildRgKey(fileId, rgId);
        RGVisibility rgVisibility = this.rgVisibilityMap.get(retinaKey);
        if (rgVisibility == null)
        {
            throw new RetinaException(String.format("RGVisibility not found for fileId: %s, rgId: %s", fileId, rgId));
        }
        return rgVisibility;
    }

    /**
     * Binary-searches {@code rgRowStart} (a sentinel-terminated cumulative array) to find
     * the RG id that contains the given global row offset.
     */
    static int rgIdForGlobalRowOffset(int globalOffset, int[] rgRowStart)
    {
        int lo = 0, hi = rgRowStart.length - 2;
        while (lo < hi)
        {
            int mid = (lo + hi + 1) >>> 1;
            if (rgRowStart[mid] <= globalOffset)
            {
                lo = mid;
            }
            else
            {
                hi = mid - 1;
            }
        }
        return lo;
    }

    /**
     * Check if the writer buffer exists for the given schema and table.
     */
    private PixelsWriteBuffer checkPixelsWriteBuffer(String schema, String table, int vNodeId) throws RetinaException
    {
        String writeBufferKey = RetinaUtils.buildWriteBufferKey(schema, table);
        Map<Integer, PixelsWriteBuffer> nodeBuffers = this.pixelsWriteBufferMap.get(writeBufferKey);
        PixelsWriteBuffer writeBuffer = nodeBuffers.get(vNodeId);
        if (writeBuffer == null)
        {
            throw new RetinaException(String.format(
                    "Writer buffer not found for vNode: %d in table: %s.%s", vNodeId, schema, table));
        }
        return writeBuffer;
    }

    /**
     * Run a full GC cycle: Memory GC → checkpoint → Storage GC.
     *
     * <p>Ordering rationale:
     * <ol>
     *   <li><b>Memory GC first</b>: {@code collectTileGarbage} compacts Deletion Chain blocks
     *       whose last item ts ≤ lwm into {@code baseBitmap}. After compaction, the remaining
     *       chain starts at the first block that straddles the lwm boundary, so the subsequent
     *       {@code getVisibilityBitmap(lwm)} call traverses at most one partial block
     *       (≤ {@code BLOCK_CAPACITY} items) instead of the entire pre-GC chain. This makes
     *       checkpoint bitmap serialisation significantly cheaper.</li>
     *   <li><b>Checkpoint second, unconditional and blocking</b>: written regardless of whether
     *       Storage GC finds any candidate files. The {@code .join()} ensures the checkpoint
     *       file is fully on disk before Storage GC begins rewriting any files, so crash
     *       recovery can always restore the post-Memory-GC visibility state independently of
     *       any in-progress Storage GC rewrite. {@code gcExecutor} is single-threaded, so the
     *       blocking join is also the simplest way to guarantee no two GC cycles overlap.</li>
     *   <li><b>Storage GC third</b>: requires an up-to-date {@code baseBitmap} (hence after
     *       Memory GC) and its own WAL for crash recovery. Placing it after the checkpoint
     *       keeps the two recovery paths independent: on restart, the GC checkpoint restores
     *       the post-Memory-GC visibility state, and the GcWal resumes any in-progress Storage
     *       GC task separately. Once scan completes, bitmaps for non-candidate files are
     *       immediately released from memory (they are no longer needed by subsequent phases).</li>
     *   <li><b>Advance {@code latestGcTimestamp} last</b>: updated only after the entire cycle
     *       succeeds (Memory GC + checkpoint + Storage GC). If any step throws, the timestamp
     *       is not advanced and the next scheduled invocation will retry the full cycle.</li>
     * </ol>
     */
    private void runGC()
    {
        processRetiredFiles();

        long timestamp = 0;
        try
        {
            timestamp = TransService.Instance().getSafeGcTimestamp();
        } catch (TransException e)
        {
            logger.error("Error while getting safe garbage collection timestamp", e);
            return;
        }

        if (timestamp <= this.latestGcTimestamp)
        {
            return;
        }

        try
        {
            // Step 1: Single pass over rgVisibilityMap — Memory GC + file-level stats
            // aggregation + CheckpointEntry pre-building.  Produces everything needed by
            // checkpoint and Storage GC without any additional traversal.
            Map<String, long[]> gcSnapshotBitmaps = new HashMap<>();
            Map<Long, long[]> fileStats = new HashMap<>();  // fileId → {totalRows, totalInvalid}
            List<CheckpointFileIO.CheckpointEntry> checkpointEntries = new ArrayList<>();

            for (Map.Entry<String, RGVisibility> entry : this.rgVisibilityMap.entrySet())
            {
                String rgKey = entry.getKey();
                long fileId = RetinaUtils.parseFileIdFromRgKey(rgKey);
                int rgId = RetinaUtils.parseRgIdFromRgKey(rgKey);

                long[] bitmap = entry.getValue().garbageCollect(timestamp);
                gcSnapshotBitmaps.put(rgKey, bitmap);

                long recordNum = entry.getValue().getRecordNum();
                long rgInvalidCount = 0;
                for (long word : bitmap)
                {
                    rgInvalidCount += Long.bitCount(word);
                }
                final long invalidCount = rgInvalidCount;

                fileStats.compute(fileId, (k, existing) -> {
                    if (existing == null)
                    {
                        return new long[]{recordNum, invalidCount};
                    }
                    existing[0] += recordNum;
                    existing[1] += invalidCount;
                    return existing;
                });

                checkpointEntries.add(
                        new CheckpointFileIO.CheckpointEntry(fileId, rgId, (int) recordNum, bitmap));
            }

            // Step 2: Checkpoint — write pre-built entries directly to disk, skipping
            // the second rgVisibilityMap traversal and per-entry thread-pool submission.
            createCheckpointDirect(timestamp, CheckpointType.GC, checkpointEntries).join();

            // Step 3: Storage GC — pass file-level stats so that candidate selection
            // uses O(1) lookups instead of per-RG aggregation loops.
            if (storageGcEnabled && storageGarbageCollector != null)
            {
                try
                {
                    storageGarbageCollector.runStorageGC(timestamp, fileStats, gcSnapshotBitmaps);
                }
                catch (Exception e)
                {
                    logger.error("Storage GC failed", e);
                }
            }

            // Step 4: Advance the timestamp only after the full cycle succeeds.
            // latestGcTimestamp is no longer updated inside createCheckpoint's async
            // callback for GC type; this is the single authoritative update point.
            long oldGcTs = this.latestGcTimestamp;
            this.latestGcTimestamp = timestamp;
            if (oldGcTs != -1 && oldGcTs != timestamp)
            {
                removeCheckpointFile(oldGcTs, CheckpointType.GC);
            }
        } catch (Exception e)
        {
            logger.error("Error while running GC", e);
        }
    }

    public void recoverCheckpoints()
    {
        try
        {
            Storage storage = StorageFactory.Instance().getStorage(checkpointDir);
            if (!storage.exists(checkpointDir))
            {
                storage.mkdirs(checkpointDir);
                return;
            }

            List<String> allFiles = storage.listPaths(checkpointDir);
            // filter only .bin files
            allFiles = allFiles.stream().filter(p -> p.endsWith(".bin")).collect(Collectors.toList());

            List<Long> gcTimestamps = new ArrayList<>();
            String offloadPrefix = RetinaUtils.getCheckpointPrefix(RetinaUtils.CHECKPOINT_PREFIX_OFFLOAD, retinaHostName);
            String gcPrefix = RetinaUtils.getCheckpointPrefix(RetinaUtils.CHECKPOINT_PREFIX_GC, retinaHostName);

            for (String path : allFiles)
            {
                // use Paths.get().getFileName() to extract filename from path string
                String filename = Paths.get(path).getFileName().toString();
                if (filename.startsWith(offloadPrefix))
                {
                    // delete offload checkpoint files when restarting
                    try
                    {
                        storage.delete(path, false);
                    } catch (IOException e)
                    {
                        logger.error("Failed to delete checkpoint file {}", path, e);
                    }
                } else if (filename.startsWith(gcPrefix))
                {
                    try
                    {
                        gcTimestamps.add(Long.parseLong(filename.replace(gcPrefix, "").replace(".bin", "")));
                    } catch (Exception e)
                    {
                        logger.error("Failed to parse checkpoint timestamp from file {}", path, e);
                    }
                }
            }

            if (gcTimestamps.isEmpty())
            {
                return;
            }

            Collections.sort(gcTimestamps);
            long latestTs = gcTimestamps.get(gcTimestamps.size() - 1);
            this.latestGcTimestamp = latestTs;
            logger.info("Loading system state from GC checkpoint: {}", latestTs);

            // load to rgVisibilityMap
            String latestPath = RetinaUtils.buildCheckpointPath(
                    checkpointDir, RetinaUtils.CHECKPOINT_PREFIX_GC, retinaHostName, latestTs);

            try
            {
                Storage latestStorage = StorageFactory.Instance().getStorage(latestPath);
                if (latestStorage.exists(latestPath))
                {
                    final long ts = latestTs;
                    int rgCount = CheckpointFileIO.readCheckpointParallel(latestPath, entry -> {
                        addVisibility(entry.fileId, entry.rgId, entry.recordNum, ts, entry.bitmap, true);
                    }, checkpointExecutor);

                    logger.info("Recovered {} RG entries from GC checkpoint", rgCount);
                }
            } catch (IOException e)
            {
                logger.error("Failed to read checkpoint file", e);
            }

            // delete old GC checkpoint files
            for (int i = 0; i < gcTimestamps.size() - 1; i++)
            {
                removeCheckpointFile(gcTimestamps.get(i), CheckpointType.GC);
            }
        } catch (IOException e)
        {
            logger.error("Failed to recover checkpoints", e);
        }
    }
}
