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
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.index.service.IndexServiceProvider;
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
import io.pixelsdb.pixels.retina.RecoveryCheckpoint.PendingSegmentEntry;
import io.pixelsdb.pixels.retina.RecoveryCheckpoint.VisibilityEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final IndexService indexService;
    private final Map<String, RGVisibility> rgVisibilityMap;
    private final Map<String, Map<Integer, PixelsWriteBuffer>> pixelsWriteBufferMap;
    private String retinaHostName;

    // GC related fields
    private final ScheduledExecutorService gcExecutor;
    private final AtomicBoolean gcScheduled;
    private final StorageGarbageCollector storageGarbageCollector;
    // Initialised by startBackgroundGc(); recovery checkpoint publication
    // is part of every GC cycle once the scheduler is running. Null until
    // then so unit/integration tests that never start the scheduler are
    // unaffected.
    private RecoveryCheckpoint recoveryCheckpoint;

    private volatile long latestGcTimestamp = -1;
    private final int totalVirtualNodeNum;

    // Offload checkpoint state (see "Offload Checkpoint Section" at the bottom of this file).
    private final String offloadCheckpointDir;
    private final ExecutorService offloadCheckpointExecutor;
    private final Map<Long, OffloadCheckpoint> offloadCheckpoints = new ConcurrentHashMap<>();

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
        /**
         * Old rowIds replaced during Storage GC index sync, forwarded verbatim from
         * {@code StorageGarbageCollector.RewriteResult#oldRowIds}.
         * <br/>
         * <b>Semantics:</b> the list length equals the corresponding
         * {@code pendingIndexEntries.size()} at syncIndex time, and may contain
         * {@code -1L} placeholders in slots where {@code updatePrimaryEntry} returned
         * a negative value (i.e. no prior entry existed to replace). Any consumer that
         * treats this list as "real rowIds to reclaim" MUST filter out negative values
         * first; e.g. a future MainIndex {@code deleteRowIdRange} based reclamation
         * must skip {@code -1L} entries.
         * <br/>
         */
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

    private RetinaResourceManager()
    {
        this.metadataService = MetadataService.Instance();
        this.indexService = IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.local);
        this.rgVisibilityMap = new ConcurrentHashMap<>();
        this.pixelsWriteBufferMap = new ConcurrentHashMap<>();

        ConfigFactory config = ConfigFactory.Instance();

        this.gcExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "retina-gc-thread");
            t.setDaemon(true);
            return t;
        });
        this.gcScheduled = new AtomicBoolean(false);
        totalVirtualNodeNum = Integer.parseInt(ConfigFactory.Instance().getProperty("node.virtual.num"));
        this.retinaHostName = NetUtils.getLocalHostName();

        this.offloadCheckpointDir = config.getProperty("retina.offload.checkpoint.dir");
        this.offloadCheckpointExecutor = Executors.newFixedThreadPool(
                Integer.parseInt(config.getProperty("retina.offload.checkpoint.threads")),
                r -> {
                    Thread t = new Thread(r, "retina-checkpoint-thread");
                    t.setDaemon(true);
                    return t;
                });

        StorageGarbageCollector gc = null;
        try
        {
            boolean storageGcEnabled = Boolean.parseBoolean(config.getProperty("retina.storage.gc.enabled"));
            if (storageGcEnabled)
            {
                double threshold = Double.parseDouble(config.getProperty("retina.storage.gc.threshold"));
                long targetFileSize = Long.parseLong(config.getProperty("retina.storage.gc.target.file.size"));
                int maxFilesPerGroup = Integer.parseInt(config.getProperty("retina.storage.gc.max.files.per.group"));
                int maxGroups = Integer.parseInt(config.getProperty("retina.storage.gc.max.file.groups.per.run"));
                int rowGroupSize = Integer.parseInt(config.getProperty("row.group.size"));
                EncodingLevel encodingLevel = EncodingLevel.from(
                        Integer.parseInt(config.getProperty("retina.storage.gc.encoding.level")));
                long retireDelayMs = (long) (Double.parseDouble(config.getProperty("retina.storage.gc.file.retire.delay.hours")) * 3_600_000L);
                gc = new StorageGarbageCollector(this, this.metadataService, this.indexService,
                        threshold, targetFileSize, maxFilesPerGroup, maxGroups,
                        rowGroupSize, encodingLevel, retireDelayMs);
                logger.info("Storage GC enabled (threshold={}, targetFileSize={}, maxFilesPerGroup={}, maxGroups={})",
                        threshold, targetFileSize, maxFilesPerGroup, maxGroups);
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to initialise StorageGarbageCollector, Storage GC will be disabled", e);
            gc = null;
        }
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

    /**
     * Starts the periodic Retina GC scheduler after the service has reached
     * the lifecycle point where background cleanup is safe to run.
     *
     * <p>The constructor intentionally does not schedule GC: startup must
     * stay fail-closed until initialization succeeds, otherwise a background
     * GC tick could observe partially constructed state. This method is
     * idempotent so callers that wire it into a service-ready hook can
     * invoke it more than once safely.</p>
     *
     * @throws RetinaException if GC configuration is invalid or the scheduler cannot be started.
     */
    public void startBackgroundGc() throws RetinaException
    {
        long interval;
        try
        {
            interval = Long.parseLong(ConfigFactory.Instance().getProperty("retina.gc.interval"));
        }
        catch (Exception e)
        {
            throw new RetinaException("Invalid retina GC interval configuration", e);
        }

        if (interval <= 0)
        {
            logger.info("Retina background GC is disabled");
            return;
        }

        if (!this.gcScheduled.compareAndSet(false, true))
        {
            logger.debug("Retina background GC scheduler has already been started");
            return;
        }

        // Fail-closed: recovery checkpoint is a durability primitive. If we
        // cannot construct it (missing/unreadable config, unreachable etcd
        // or storage backend), refuse to start the GC scheduler rather than
        // silently run without crash recovery.
        this.recoveryCheckpoint = RecoveryCheckpoint.createDefault();

        try
        {
            this.gcExecutor.scheduleAtFixedRate(
                    this::runGC,
                    interval,
                    interval,
                    TimeUnit.SECONDS
            );
            logger.info("Retina background GC scheduler started with interval {} seconds", interval);
        }
        catch (RuntimeException e)
        {
            this.gcScheduled.set(false);
            throw new RetinaException("Failed to start retina background GC", e);
        }
    }

    public boolean isBackgroundGcStarted()
    {
        return this.gcScheduled.get();
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

    public void removeVisibility(long fileId)
    {
        String prefix = fileId + "_";
        this.rgVisibilityMap.entrySet().removeIf(entry ->
        {
            if (!entry.getKey().startsWith(prefix))
            {
                return false;
            }
            RGVisibility rgVisibility = entry.getValue();
            if (rgVisibility != null)
            {
                rgVisibility.close();
            }
            return true;
        });
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

    public void deleteRecord(long fileId, int rgId, int rgRowOffset, long timestamp) throws RetinaException
    {
        deleteRecord(fileId, rgId, rgRowOffset, timestamp, RGVisibility.ReplayMode.NORMAL);
    }

    public void deleteRecord(long fileId, int rgId, int rgRowOffset, long timestamp,
                             RGVisibility.ReplayMode replayMode) throws RetinaException
    {
        checkRGVisibility(fileId, rgId).deleteRecord(rgRowOffset, timestamp, replayMode);

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
                        checkRGVisibility(bwd.oldFileId, oldRgId).deleteRecord(oldRgOff, timestamp, replayMode);
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
                    checkRGVisibility(result.newFileId, newRgId).deleteRecord(newRgOff, timestamp, replayMode);
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

    public void deleteRecord(IndexProto.RowLocation rowLocation, long timestamp,
                             RGVisibility.ReplayMode replayMode)
            throws RetinaException
    {
        deleteRecord(rowLocation.getFileId(), rowLocation.getRgId(), rowLocation.getRgRowOffset(),
                timestamp, replayMode);
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

    public IndexProto.PrimaryIndexEntry.Builder insertRecord(String schemaName, String tableName,
                                                             byte[][] colValues, long timestamp,
                                                             int vNodeId) throws RetinaException
    {
        IndexProto.PrimaryIndexEntry.Builder builder = IndexProto.PrimaryIndexEntry.newBuilder();
        PixelsWriteBuffer writeBuffer = checkPixelsWriteBuffer(schemaName, tableName, vNodeId);
        builder.setRowId(writeBuffer.addRow(colValues, timestamp, builder.getRowLocationBuilder()));
        return builder;
    }

    private RetinaProto.VisibilityBitmap getVisibilityBitmapSlice(long[] visibilityBitmap, long startIndex, int length) throws RetinaException
    {
        if (startIndex % 64 != 0)
        {
            throw new RetinaException("StartIndex must be multiple of 64");
        }
        if (length <= 0)
        {
            return RetinaProto.VisibilityBitmap.newBuilder().build();
        }

        int alignedLength = ((length + 63) / 64) * 64;
        int startLongIndex = (int) (startIndex / 64);
        int endLongIndex = startLongIndex + (alignedLength / 64);

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

        // Active memTable returns its full appended rows; visibility is masked
        // downstream by the RGVisibility bitmap slice below.
        int activeSize = activeMemtable.getSize();
        if (activeSize > 0)
        {
            ByteString data = ByteString.copyFrom(activeMemtable.serialize());
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
            if (!immutableMemtable.isEmpty())
            {
                fileIds.add(immutableMemtable.getFileId());
                ids.add(immutableMemtable.getId());
            }
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

        // only return the corresponding visible part of bitmap
        if (activeSize > 0)
        {
            responseBuilder.addBitmaps(getVisibilityBitmapSlice(
                    fileIdToVisibility.get(activeMemtable.getFileId()),
                    activeMemtable.getStartIndex(), activeSize));
        } else
        {
            responseBuilder.addBitmaps(RetinaProto.VisibilityBitmap.newBuilder());
        }
        for (MemTable immutableMemtable : immutableMemTables)
        {
            int immutableSize = immutableMemtable.getSize();
            if (immutableSize > 0)
            {
                responseBuilder.addBitmaps(getVisibilityBitmapSlice(
                        fileIdToVisibility.get(immutableMemtable.getFileId()),
                        immutableMemtable.getStartIndex(), immutableSize));
            }
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
     * Run a full GC cycle: Memory GC → Storage GC → Recovery Checkpoint.
     *
     * <p>Ordering rationale:
     * <ol>
     *   <li><b>Memory GC first</b>: {@code collectTileGarbage} compacts Deletion Chain blocks
     *       whose last item ts ≤ the safe folding timestamp into {@code baseBitmap}. After compaction,
     *       the remaining chain starts at the first block that straddles that boundary, so the subsequent
     *       {@code getVisibilityBitmap(timestamp)} call traverses at most one partial block
     *       (≤ {@code BLOCK_CAPACITY} items) instead of the entire pre-GC chain. The same pass
     *       also captures one {@link VisibilityEntry} per RG by reusing the post-fold bitmap,
     *       so Recovery Checkpoint does not have to traverse RGVisibility a second time.</li>
     *   <li><b>Storage GC second</b>: requires an up-to-date {@code baseBitmap} (hence after
     *       Memory GC) and its own WAL to resume in-progress tasks after a crash. Once scan
     *       completes, bitmaps for non-candidate files are immediately released from memory
     *       (they are no longer needed by subsequent phases).</li>
     *   <li><b>Recovery Checkpoint third</b>: receives the {@code rgEntries} collected in
     *       Step 1 plus per-scope earliest pending commit timestamps, then publishes the
     *       body + etcd pointer. Unlike Storage GC, a publish failure here aborts the cycle:
     *       the outer catch skips the {@code latestGcTimestamp} advancement, and the next
     *       cycle retries the full sequence so crash recovery never silently lags.</li>
     *   <li><b>Advance {@code latestGcTimestamp} last</b>: updated only after Memory GC and
     *       Recovery Checkpoint both succeed. Storage GC failures do not block advancement
     *       because compaction is opportunistic.</li>
     * </ol>
     */
    private void runGC()
    {
        processRetiredFiles();

        long timestamp = 0;
        try
        {
            timestamp = TransService.Instance().getSafeVisibilityFoldingTimestamp(true);
        } catch (TransException e)
        {
            logger.error("Error while getting safe visibility folding timestamp", e);
            return;
        }

        if (timestamp <= this.latestGcTimestamp)
        {
            return;
        }

        try
        {
            // Step 1: Single pass over rgVisibilityMap — Memory GC + file-level stats
            // aggregation + Recovery Checkpoint entries. Produces everything needed by
            // Storage GC and Recovery Checkpoint without any additional traversal of
            // rgVisibilityMap or extra native-side bitmap reads.
            Map<String, long[]> gcSnapshotBitmaps = new HashMap<>();
            Map<Long, long[]> fileStats = new HashMap<>();  // fileId → {totalRows, totalInvalid}
            List<VisibilityEntry> rgEntries = new ArrayList<>(this.rgVisibilityMap.size());

            for (Map.Entry<String, RGVisibility> entry : this.rgVisibilityMap.entrySet())
            {
                String rgKey = entry.getKey();
                long fileId = RetinaUtils.parseFileIdFromRgKey(rgKey);
                int rgId = RetinaUtils.parseRgIdFromRgKey(rgKey);
                RGVisibility rgVisibility = entry.getValue();

                long[] bitmap = rgVisibility.garbageCollect(timestamp);
                gcSnapshotBitmaps.put(rgKey, bitmap);

                long recordNum = rgVisibility.getRecordNum();
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

                // Reuse the post-fold bitmap as the checkpoint entry's bitmap: it
                // already reflects every delete with delete_ts <= timestamp folded
                // into base, which is exactly what the loader needs to rebuild
                // RGVisibility with an empty deletion chain.
                rgEntries.add(new VisibilityEntry(fileId, rgId, (int) recordNum, timestamp, bitmap));
            }

            // Step 2: Storage GC — pass file-level stats so that candidate selection
            // uses O(1) lookups instead of per-RG aggregation loops.
            if (storageGarbageCollector != null)
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

            // Step 3: Publish a recovery checkpoint at the same timestamp the
            // Memory GC just folded against, reusing the rgEntries already
            // collected in Step 1. Unlike Storage GC failures (which we swallow
            // because compaction is opportunistic), checkpoint publication
            // failures must propagate: the outer catch will skip the
            // latestGcTimestamp advancement so the next cycle retries.
            if (recoveryCheckpoint != null)
            {
                // Project per-scope earliest pending commit ts. Buffers with
                // ts == Long.MAX_VALUE have no committed pending data and are
                // omitted: the scope contributes nothing to recovery replay.
                List<PendingSegmentEntry> segments = new ArrayList<>();
                for (Map<Integer, PixelsWriteBuffer> perTable : this.pixelsWriteBufferMap.values())
                {
                    for (PixelsWriteBuffer buffer : perTable.values())
                    {
                        long ts = buffer.getEarliestPendingMinTs();
                        if (ts != Long.MAX_VALUE)
                        {
                            segments.add(new PendingSegmentEntry(buffer.getTableId(),
                                    buffer.getVirtualNodeId(), ts));
                        }
                    }
                }
                recoveryCheckpoint.generate(timestamp, rgEntries, segments);
            }

            // Step 4: Advance the timestamp only after the full cycle succeeds.
            this.latestGcTimestamp = timestamp;
        } catch (Exception e)
        {
            logger.error("Error while running GC", e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Offload Checkpoint Section
    //
    // Long-running queries register an "offload" status with a logical
    // timestamp; this section materialises one visibility checkpoint file per
    // registered timestamp and reference-counts concurrent registrations so
    // that the file is created exactly once and deleted only after the last
    // unregistration.
    //
    // State lives in three RRM fields declared at the top of the class:
    // offloadCheckpointDir, offloadCheckpointExecutor, offloadCheckpoints.
    // ─────────────────────────────────────────────────────────────────────

    /**
     * Per-timestamp state aggregating reference count, in-flight creation
     * future, and the resulting file path. Doubles as the synchronization
     * monitor for all transitions on this timestamp's lifecycle.
     */
    private static final class OffloadCheckpoint
    {
        final AtomicInteger refCount = new AtomicInteger(0);
        /** Set once createOffloadCheckpoint successfully commits the file; null otherwise. */
        volatile String filePath;
        /** Tracks the in-flight creation task; cleared lazily on retry after failure. */
        volatile CompletableFuture<Void> future;
    }

    /**
     * Long-running queries register an "Offload" status to ensure that the
     * required visibility checkpoint is created. Concurrent registrations of
     * the same timestamp are reference-counted and share a single checkpoint
     * file, which has virtually no impact on queries since long-running
     * transactions do not need newly written data.
     */
    public void registerOffload(long timestamp) throws RetinaException
    {
        OffloadCheckpoint cp = offloadCheckpoints.computeIfAbsent(timestamp, k -> new OffloadCheckpoint());
        CompletableFuture<Void> future;

        synchronized (cp)
        {
            cp.refCount.incrementAndGet();

            if (cp.filePath != null)
            {
                logger.info("Registered offload for Timestamp: {} (already exists)", timestamp);
                return;
            }

            future = cp.future;
            if (future != null && future.isCompletedExceptionally())
            {
                // Previous attempt failed; drop the stale future so this caller retries.
                cp.future = null;
                future = null;
            }

            if (future == null)
            {
                future = createOffloadCheckpoint(timestamp, cp);
                cp.future = future;
            }
        }

        try
        {
            future.join();
            logger.info("Registered offload for Timestamp: {}", timestamp);
        }
        catch (Exception e)
        {
            synchronized (cp)
            {
                cp.refCount.decrementAndGet();
            }
            throw new RetinaException("Failed to create checkpoint for timestamp: " + timestamp, e);
        }
    }

    public void unregisterOffload(long timestamp)
    {
        OffloadCheckpoint cp = offloadCheckpoints.get(timestamp);
        if (cp == null)
        {
            return;
        }
        synchronized (cp)
        {
            if (cp.refCount.decrementAndGet() > 0)
            {
                return;
            }
            offloadCheckpoints.remove(timestamp);
            deleteOffloadCheckpoint(timestamp);
            logger.info("Offload checkpoint for timestamp {} removed.", timestamp);
        }
    }

    public String getOffloadCheckpointPath(long timestamp)
    {
        OffloadCheckpoint cp = offloadCheckpoints.get(timestamp);
        return cp == null ? null : cp.filePath;
    }

    /**
     * Cleans up stale offload checkpoint files left over by previous runs of
     * this node before the service opens for queries. Long-running queries
     * that owned those checkpoints are no longer active after a restart, so
     * the files are safe to drop.
     *
     * <p>Cross-restart visibility recovery is the responsibility of the
     * recovery checkpoint flow (see {@code recovery.md}); this method does
     * not rebuild {@code rgVisibilityMap}.
     */
    public void recoverOffloadCheckpoints()
    {
        try
        {
            Storage storage = StorageFactory.Instance().getStorage(offloadCheckpointDir);
            if (!storage.exists(offloadCheckpointDir))
            {
                storage.mkdirs(offloadCheckpointDir);
                return;
            }

            String offloadPrefix = RetinaUtils.getCheckpointPrefix(
                    RetinaUtils.CHECKPOINT_PREFIX_OFFLOAD, retinaHostName);
            for (String path : storage.listPaths(offloadCheckpointDir))
            {
                if (!path.endsWith(".bin"))
                {
                    continue;
                }
                String filename = Paths.get(path).getFileName().toString();
                if (!filename.startsWith(offloadPrefix))
                {
                    continue;
                }
                try
                {
                    storage.delete(path, false);
                }
                catch (IOException e)
                {
                    logger.error("Failed to delete stale offload checkpoint file {}", path, e);
                }
            }
        }
        catch (IOException e)
        {
            logger.error("Failed to recover offload checkpoints", e);
        }
    }

    /**
     * Two-phase checkpoint creation:
     * <ol>
     *   <li>Fold each RG's deletion chain at {@code timestamp} in parallel.
     *       A failure in any fold task surfaces through the returned future
     *       (no swallowed errors, no waiting on the writer's 60s timeout).</li>
     *   <li>Once all bitmaps are ready, drain them into the queue and write
     *       the file. On any failure the partial file is removed via the
     *       {@code whenComplete} side effect.</li>
     * </ol>
     *
     * <p>Concurrency safety: {@link #registerOffload} guarantees at most one
     * in-flight future per OffloadCheckpoint via {@code synchronized(cp)} +
     * single-writer of {@code cp.future}, so no file-level locking is needed.
     */
    private CompletableFuture<Void> createOffloadCheckpoint(long timestamp, OffloadCheckpoint cp)
    {
        String filePath = RetinaUtils.buildCheckpointPath(
                offloadCheckpointDir, RetinaUtils.CHECKPOINT_PREFIX_OFFLOAD, retinaHostName, timestamp);

        List<Map.Entry<String, RGVisibility>> entries = new ArrayList<>(rgVisibilityMap.entrySet());
        int totalRgs = entries.size();
        logger.info("Starting offload checkpoint for {} RGs at timestamp {}", totalRgs, timestamp);

        List<CompletableFuture<CheckpointFileIO.CheckpointEntry>> bitmapFutures = new ArrayList<>(totalRgs);
        for (Map.Entry<String, RGVisibility> entry : entries)
        {
            bitmapFutures.add(CompletableFuture.supplyAsync(() -> {
                String key = entry.getKey();
                long fileId = RetinaUtils.parseFileIdFromRgKey(key);
                int rgId = RetinaUtils.parseRgIdFromRgKey(key);
                RGVisibility rgVisibility = entry.getValue();
                long[] bitmap = rgVisibility.getVisibilityBitmap(timestamp);
                return new CheckpointFileIO.CheckpointEntry(
                        fileId, rgId, (int) rgVisibility.getRecordNum(), bitmap);
            }, offloadCheckpointExecutor));
        }

        return CompletableFuture
                .allOf(bitmapFutures.toArray(new CompletableFuture[0]))
                .thenRunAsync(() -> {
                    long startWrite = System.currentTimeMillis();
                    BlockingQueue<CheckpointFileIO.CheckpointEntry> queue =
                            new ArrayBlockingQueue<>(Math.max(1, totalRgs));
                    try
                    {
                        for (CompletableFuture<CheckpointFileIO.CheckpointEntry> f : bitmapFutures)
                        {
                            queue.put(f.join());
                        }
                        CheckpointFileIO.writeCheckpoint(filePath, totalRgs, queue);
                        long endWrite = System.currentTimeMillis();
                        logger.info("Writing offload checkpoint file to {} took {} ms",
                                filePath, (endWrite - startWrite));
                        cp.filePath = filePath;
                    }
                    catch (Exception e)
                    {
                        throw new CompletionException(e);
                    }
                }, offloadCheckpointExecutor)
                .whenComplete((unused, throwable) -> {
                    if (throwable != null)
                    {
                        logger.error("Failed to create offload checkpoint for timestamp: {}",
                                timestamp, throwable);
                        deleteOffloadCheckpoint(timestamp);
                    }
                });
    }

    private void deleteOffloadCheckpoint(long timestamp)
    {
        String path = RetinaUtils.buildCheckpointPath(
                offloadCheckpointDir, RetinaUtils.CHECKPOINT_PREFIX_OFFLOAD, retinaHostName, timestamp);

        try
        {
            Storage storage = StorageFactory.Instance().getStorage(path);
            if (storage.exists(path))
            {
                storage.delete(path, false);
            }
        }
        catch (IOException e)
        {
            logger.warn("Failed to delete offload checkpoint file {}", path, e);
        }
    }
}
