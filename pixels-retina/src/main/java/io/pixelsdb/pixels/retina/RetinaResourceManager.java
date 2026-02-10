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
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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

    // Checkpoint related fields
    private final ExecutorService checkpointExecutor;
    private final Map<Long, String> offloadedCheckpoints;
    private final Cache<Long, Map<String, long[]>> offloadCache;
    private final String checkpointDir;
    private long latestGcTimestamp = -1;
    private final int totalVirtualNodeNum;

    private final Map<Long, AtomicInteger> checkpointRefCounts;

    private static class RecoveredState
    {
        final long timestamp;
        final long[] bitmap;

        RecoveredState(long timestamp, long[] bitmap)
        {
            this.timestamp = timestamp;
            this.bitmap = bitmap;
        }
    }
    private final Map<String, RecoveredState> recoveryCache;

    private static class CheckpointEntry
    {
        final long fileId;
        final int rgId;
        final long[] bitmap;

        CheckpointEntry(long fileId, int rgId, long[] bitmap)
        {
            this.fileId = fileId;
            this.rgId = rgId;
            this.bitmap = bitmap;
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

        ConfigFactory config = ConfigFactory.Instance();
        long leaseDuration = Long.parseLong(config.getProperty("retina.offload.cache.lease.duration"));

        this.offloadCache = Caffeine.newBuilder()
                .expireAfterAccess(leaseDuration, TimeUnit.SECONDS)
                .removalListener((key, value, cause) -> {
                    logger.info("Retina offload cache for timestamp {} evicted due to {}", key, cause);
                })
                .build();

        this.checkpointRefCounts = new ConcurrentHashMap<>();
        this.checkpointDir = config.getProperty("pixels.retina.checkpoint.dir");
        this.recoveryCache = new ConcurrentHashMap<>();

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
        this.retinaHostName = System.getenv("HOSTNAME");
        if (retinaHostName == null)
        {
            try
            {
                this.retinaHostName = InetAddress.getLocalHost().getHostName();
                logger.debug("HostName from InetAddress: {}", retinaHostName);
            } catch (UnknownHostException e)
            {
                logger.error("Failed to get retina hostname", e);
            }
        }
    }

    private static final class InstanceHolder
    {
        private static final RetinaResourceManager instance = new RetinaResourceManager();
    }

    public static RetinaResourceManager Instance()
    {
        return InstanceHolder.instance;
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
            String offloadPrefix = "vis_offload_" + retinaHostName + "_";
            String gcPrefix = "vis_gc_" + retinaHostName + "_";

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

            // load to recoveryCache
            loadCheckpointToCache(latestTs);

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

    private void loadCheckpointToCache(long timestamp)
    {
        String fileName = "vis_gc_" + retinaHostName + "_" + timestamp + ".bin";
        // construct path. Storage expects '/' separator usually, but let's be safe
        String path = checkpointDir.endsWith("/") ? checkpointDir + fileName : checkpointDir + "/" + fileName;

        try
        {
            Storage storage = StorageFactory.Instance().getStorage(path);
            if (!storage.exists(path))
            {
                return;
            }

            try (DataInputStream in = storage.open(path))
            {
                int rgCount = in.readInt();
                for (int i = 0; i < rgCount; i++)
                {
                    long fileId = in.readLong();
                    int rgId = in.readInt();
                    int len = in.readInt();
                    long[] bitmap = new long[len];
                    for (int j = 0; j < len; j++)
                    {
                        bitmap[j] = in.readLong();
                    }
                    recoveryCache.put(fileId + "_" + rgId, new RecoveredState(timestamp, bitmap));
                }
            }
        } catch (IOException e)
        {
            logger.error("Failed to read checkpoint file: {}", e);
        }
    }

    public void addVisibility(long fileId, int rgId, int recordNum)
    {
        String rgKey = fileId + "_" + rgId;
        RecoveredState recoveredState = recoveryCache.remove(rgKey);

        RGVisibility rgVisibility;
        if (recoveredState != null)
        {
            rgVisibility = new RGVisibility(recordNum, recoveredState.timestamp, recoveredState.bitmap);
        } else
        {
            rgVisibility = new RGVisibility(recordNum);
        }
        rgVisibilityMap.put(rgKey, rgVisibility);
    }

    public void finishRecovery()
    {
        if (!recoveryCache.isEmpty())
        {
            logger.info("Dropping {} orphaned entries from recovery cache.", recoveryCache.size());
            recoveryCache.clear();
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
                    addVisibility(fileId, rgId, recordNum);
                }
            }
        } catch (Exception e)
        {
            throw new RetinaException("Failed to add visibility for file " +  filePath, e);
        }
    }

    public long[] queryVisibility(long fileId, int rgId, long timestamp, long transId) throws RetinaException
    {
        // [Routing Logic] Only read from disk/cache if the transaction is explicitly registered as Offload
        if (transId != -1)
        {
            if (offloadedCheckpoints.containsKey(timestamp))
            {
                return queryFromOffloadCache(timestamp, fileId, rgId);
            }
            throw new RetinaException("Offloaded checkpoint missing for TransID " + transId);
        }
        // otherwise read from memory
        RGVisibility rgVisibility = checkRGVisibility(fileId, rgId);
        long[] visibilityBitmap = rgVisibility.getVisibilityBitmap(timestamp);
        if (visibilityBitmap == null)
        {
            throw new RetinaException(String.format("Failed to get visibility for fileId: %d, rgId: %d", fileId, rgId));
        }
        return visibilityBitmap;
    }

    private long[] queryFromOffloadCache(long timestamp, long fileId, int rgId) throws RetinaException
    {
        Map<String, long[]> cache = offloadCache.getIfPresent(timestamp);
        if (cache != null)
        {
            long[] bitmap = cache.get(fileId + "_" + rgId);
            if (bitmap != null)
            {
                return bitmap;
            }
        }
        // Cache miss, load from storage
        return loadBitmapFromStorage(timestamp, fileId, rgId);
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
        while (true)
        {
            AtomicInteger refCount = checkpointRefCounts.computeIfAbsent(timestamp, k -> new AtomicInteger(0));

            CompletableFuture<Void> currentFuture = null;
            synchronized (refCount)
            {
                if (checkpointRefCounts.get(timestamp) != refCount)
                {
                    continue;
                }

                int currentRef = refCount.incrementAndGet();
                if (currentRef == 1)
                {
                    try
                    {
                        if (!offloadedCheckpoints.containsKey(timestamp))
                        {
                            currentFuture = createCheckpoint(timestamp, CheckpointType.OFFLOAD);
                        }
                    } catch (Exception e)
                    {
                        refCount.decrementAndGet();
                        if (e instanceof RuntimeException) throw (RuntimeException) e;
                        throw new RetinaException("Failed to register offload", e);
                    }
                }
            }

            if (currentFuture != null)
            {
                try
                {
                    currentFuture.join();
                } catch (Exception e)
                {
                    // Checkpoint creation failed
                    synchronized (refCount)
                    {
                        refCount.decrementAndGet();
                    }
                    throw new RetinaException("Failed to create checkpoint", e);
                }
            }
            else
            {
                // Wait for the thread that's currently creating the checkpoint
                while (!offloadedCheckpoints.containsKey(timestamp) && checkpointRefCounts.containsKey(timestamp))
                {
                    try
                    {
                        Thread.sleep(100);
                    } catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        throw new RetinaException("Interrupted while waiting for offload", e);
                    }
                }
                if (!offloadedCheckpoints.containsKey(timestamp))
                {
                    // Maybe the other thread failed or it was unregistered immediately
                    continue;
                }
            }

            logger.info("Registered offload for Timestamp: {}", timestamp);
            return;
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
                    offloadCache.invalidate(timestamp);
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
        String prefix = (type == CheckpointType.GC) ? "vis_gc_" : "vis_offload_";
        String fileName = prefix + retinaHostName + "_" + timestamp + ".bin";
        String filePath = checkpointDir.endsWith("/") ? checkpointDir + fileName : checkpointDir + "/" + fileName;

        // 1. Capture current entries to ensure we process a consistent set of RGs
        List<Map.Entry<String, RGVisibility>> entries = new ArrayList<>(this.rgVisibilityMap.entrySet());
        int totalRgs = entries.size();
        logger.info("Starting {} checkpoint for {} RGs at timestamp {}", type, totalRgs, timestamp);

        // 2. Use a BlockingQueue for producer-consumer pattern
        // Limit capacity to avoid excessive memory usage if writing is slow
        BlockingQueue<CheckpointEntry> queue = new LinkedBlockingQueue<>(1024);

        // 3. Start producer tasks to fetch bitmaps in parallel
        for (Map.Entry<String, RGVisibility> entry : entries)
        {
            checkpointExecutor.submit(() -> {
                try
                {
                    String key = entry.getKey();
                    String[] parts = key.split("_");
                    long fileId = Long.parseLong(parts[0]);
                    int rgId = Integer.parseInt(parts[1]);
                    long[] bitmap = entry.getValue().getVisibilityBitmap(timestamp);
                    queue.put(new CheckpointEntry(fileId, rgId, bitmap));
                } catch (Exception e)
                {
                    logger.error("Failed to fetch visibility bitmap for checkpoint", e);
                }
            });
        }

        // 4. Async Write: perform IO in background thread (Consumer)
        // Use commonPool to avoid deadlocks with checkpointExecutor
        return CompletableFuture.runAsync(() -> {
            long startWrite = System.currentTimeMillis();
            try
            {
                Storage storage = StorageFactory.Instance().getStorage(filePath);
                try (DataOutputStream out = storage.create(filePath, true, 8 * 1024 * 1024))
                {
                    out.writeInt(totalRgs);
                    for (int i = 0; i < totalRgs; i++)
                    {
                        CheckpointEntry entry = queue.take();
                        out.writeLong(entry.fileId);
                        out.writeInt(entry.rgId);
                        out.writeInt(entry.bitmap.length);
                        for (long l : entry.bitmap)
                        {
                            out.writeLong(l);
                        }
                    }
                    out.flush();
                }
                long endWrite = System.currentTimeMillis();
                logger.info("Writing {} checkpoint file to {} took {} ms", type, filePath, (endWrite - startWrite));

                if (type == CheckpointType.OFFLOAD)
                {
                    offloadedCheckpoints.put(timestamp, filePath);
                } else
                {
                    long oldGcTs = this.latestGcTimestamp;
                    this.latestGcTimestamp = timestamp;
                    if (oldGcTs != -1 && oldGcTs != timestamp)
                    {
                        removeCheckpointFile(oldGcTs, CheckpointType.GC);
                    }
                }
            } catch (Exception e)
            {
                logger.error("Failed to commit {} checkpoint file for timestamp: {}", type, timestamp, e);
                // Try to cleanup the potentially corrupted or partial file
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

    private long[] loadBitmapFromStorage(long timestamp, long targetFileId, int targetRgId) throws RetinaException
    {
        String path = offloadedCheckpoints.get(timestamp);
        if (path == null)
        {
            throw new RetinaException("Checkpoint missing: " + timestamp);
        }

        // Use a lock to ensure only one thread parses the file for this timestamp
        // For 50,000 RGs, the initial capacity of ConcurrentHashMap should be sufficient
        Map<String, long[]> cache;
        synchronized (offloadCache)
        {
            cache = offloadCache.getIfPresent(timestamp);
            if (cache == null)
            {
                cache = new ConcurrentHashMap<>(50000);
                offloadCache.put(timestamp, cache);
            }
        }

        synchronized (cache)
        {
            // Double check if target already in cache after acquiring lock
            long[] cached = cache.get(targetFileId + "_" + targetRgId);
            if (cached != null)
            {
                return cached;
            }

            // Still not in cache, perform full load
            try
            {
                Storage storage = StorageFactory.Instance().getStorage(path);
                long fileLength = storage.getStatus(path).getLength();

                // Use PhysicalReader to read the entire file into memory at once
                // This is much faster than streaming from S3 for many small reads
                byte[] content;
                try (PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(storage, path))
                {
                    ByteBuffer buffer = reader.readFully((int) fileLength);
                    if (buffer.hasArray())
                    {
                        content = buffer.array();
                    }
                    else
                    {
                        content = new byte[(int) fileLength];
                        buffer.get(content);
                    }
                }

                try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(content)))
                {
                    int rgCount = in.readInt();

                    for (int i = 0; i < rgCount; i++)
                    {
                        long fileId = in.readLong();
                        int rgId = in.readInt();
                        int len = in.readInt();
                        long[] bitmap = new long[len];
                        for (int j = 0; j < len; j++)
                        {
                            bitmap[j] = in.readLong();
                        }
                        
                        cache.put(fileId + "_" + rgId, bitmap);
                    }
                }
            } catch (IOException e)
            {
                throw new RetinaException("Failed to read checkpoint file", e);
            }
        }

        return cache.getOrDefault(targetFileId + "_" + targetRgId, new long[0]);
    }

    private void removeCheckpointFile(long timestamp, CheckpointType type)
    {
        String prefix = (type == CheckpointType.GC) ? "vis_gc_" : "vis_offload_";
        String fileName = prefix + retinaHostName + "_" + timestamp + ".bin";
        String path = checkpointDir.endsWith("/") ? checkpointDir + fileName : checkpointDir + "/" + fileName;

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
        RGVisibility rgVisibility = checkRGVisibility(fileId, rgId);
        rgVisibility.getVisibilityBitmap(timestamp);
    }

    public void deleteRecord(long fileId, int rgId, int rgRowOffset, long timestamp) throws RetinaException
    {
        RGVisibility rgVisibility = checkRGVisibility(fileId, rgId);
        rgVisibility.deleteRecord(rgRowOffset, timestamp);
    }

    public void deleteRecord(IndexProto.RowLocation rowLocation, long timestamp) throws RetinaException
    {
        deleteRecord(rowLocation.getFileId(), rowLocation.getRgId(), rowLocation.getRgRowOffset(), timestamp);
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

            String writeBufferKey = schemaName + "_" + tableName;
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
        String retinaKey = fileId + "_" + rgId;
        RGVisibility rgVisibility = this.rgVisibilityMap.get(retinaKey);
        if (rgVisibility == null)
        {
            throw new RetinaException(String.format("RGVisibility not found for fileId: %s, rgId: %s", fileId, rgId));
        }
        return rgVisibility;
    }

    /**
     * Check if the writer buffer exists for the given schema and table.
     */
    private PixelsWriteBuffer checkPixelsWriteBuffer(String schema, String table, int vNodeId) throws RetinaException
    {
        String writeBufferKey = schema + "_" + table;
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
     * Run garbage collection on all registered RGVisibility.
     */
    private void runGC()
    {
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
            // 1. Persist first
            createCheckpoint(timestamp, CheckpointType.GC);
            // 2. Then clean memory
            for (Map.Entry<String, RGVisibility> entry: this.rgVisibilityMap.entrySet())
            {
                RGVisibility rgVisibility = entry.getValue();
                rgVisibility.garbageCollect(timestamp);
            }
        } catch (Exception e)
        {
            logger.error("Error while running GC", e);
        }
    }
}
