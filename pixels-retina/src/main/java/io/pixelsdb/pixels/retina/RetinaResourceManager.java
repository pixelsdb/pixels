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
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Use the singleton pattern to manage data resources in the retina service.
 */
public class RetinaResourceManager
{
    private static final Logger logger = LogManager.getLogger(RetinaResourceManager.class);
    private final MetadataService metadataService;
    private final Map<String, RGVisibility> rgVisibilityMap;
    private final Map<String, PixelsWriteBuffer> pixelsWriteBufferMap;
    private String retinaHostName;

    // GC related fields
    private final ScheduledExecutorService gcExecutor;

    // Checkpoint related fields
    private final Map<Long, Path> offloadedCheckpoints;
    private final Path checkpointDir;
    private long latestGcTimestamp = -1;

    private final Set<Long> offloadedTransIds; // identity routing whitelist
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
        this.offloadedTransIds = ConcurrentHashMap.newKeySet();
        this.checkpointRefCounts = new ConcurrentHashMap<>();
        this.checkpointDir = Paths.get(ConfigFactory.Instance().getProperty("pixels.retina.checkpoint.dir"));
        this.recoveryCache = new ConcurrentHashMap<>();

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "retina-gc-thread");
                    t.setDaemon(true);
                    return t;
                });
        try
        {
            ConfigFactory config = ConfigFactory.Instance();
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
            if (!Files.exists(checkpointDir))
            {
                Files.createDirectories(checkpointDir);
                return;
            }
            List<Path> allFiles;
            try (Stream<Path> stream = Files.list(checkpointDir))
            {
                allFiles = stream.filter(p -> p.toString().endsWith(".bin")).collect(Collectors.toList());
            }

            List<Long> gcTimestamps = new ArrayList<>();
            for (Path path : allFiles)
            {
                String filename = path.getFileName().toString();
                if (filename.startsWith("vis_offload_"))
                {
                    // delete offload checkpoint files when restarting
                    try
                    {
                        Files.deleteIfExists(path);
                    } catch (IOException e)
                    {
                        logger.error("Failed to delete checkpoint file {}", path, e);
                    }
                } else if (filename.startsWith("vis_gc_"))
                {
                    try
                    {
                        gcTimestamps.add(Long.parseLong(filename.replace("vis_gc_", "").replace(".bin", "")));
                    } catch (Exception e)
                    {
                        logger.error("Failed to delete checkpoint file {}", path, e);
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
        String fileName = "vis_gc_" + timestamp + ".bin";
        Path path = checkpointDir.resolve(fileName);
        if (!Files.exists(path))
        {
            return;
        }

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path))))
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
        // [Routing Logic] Only read from disk if the transaction is explicitly registered as Offload
        if (transId != -1 && offloadedTransIds.contains(transId))
        {
            if (offloadedCheckpoints.containsKey(timestamp))
            {
                return loadBitmapFromDisk(timestamp, fileId, rgId);
            }
            logger.error("Offloaded checkpoint missing for TransID {}, falling back to memory.", transId);
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
     * @param transId
     * @param timestamp
     * @throws RetinaException
     */
    public void registerOffload(long transId, long timestamp) throws RetinaException
    {
        while (true)
        {
            AtomicInteger refCount = checkpointRefCounts.computeIfAbsent(timestamp, k -> new AtomicInteger(0));

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
                            createDiskCheckpoint(timestamp, CheckpointType.OFFLOAD);
                        }
                    } catch (Exception e)
                    {
                        refCount.decrementAndGet();
                        throw e;
                    }
                }
            }
            offloadedTransIds.add(transId);
            logger.info("Registered offload for TransID: {}, Timestamp: {}", transId, timestamp);
            return;
        }
    }

    public void unregisterOffload(long transId, long timestamp)
    {
        if (offloadedTransIds.remove(transId))
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
    }

    private void createDiskCheckpoint(long timestamp, CheckpointType type) throws RetinaException
    {
        String prefix = (type == CheckpointType.GC) ? "vis_gc_" : "vis_offload_";
        String fileName = prefix + timestamp + ".bin";
        Path filePath = checkpointDir.resolve(fileName);
        Path tempPath = checkpointDir.resolve(fileName + ".tmp");

        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(tempPath))))
        {
            int rgCount = this.rgVisibilityMap.size();
            out.writeInt(rgCount);
            for (Map.Entry<String, RGVisibility> entry : this.rgVisibilityMap.entrySet())
            {
                String[] parts = entry.getKey().split("_");
                long fileId = Long.parseLong(parts[0]);
                int rgId = Integer.parseInt(parts[1]);
                long[] bitmap = entry.getValue().getVisibilityBitmap(timestamp);

                out.writeLong(fileId);
                out.writeInt(rgId);
                out.writeInt(bitmap.length);
                for (long l : bitmap)
                {
                    out.writeLong(l);
                }
            }
            out.flush();
        } catch (IOException e)
        {
            try
            {
                Files.deleteIfExists(tempPath);
            } catch (IOException ignored)
            {
            }
            throw new RetinaException("Failed to write checkpoint file", e);
        }

        try
        {
            StandardCopyOption[] options = new StandardCopyOption[]{
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            };
            Files.move(tempPath, filePath, options);
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
        } catch (IOException e)
        {
            throw new RetinaException("Failed to commit checkpoint file", e);
        }
    }

    private long[] loadBitmapFromDisk(long timestamp, long targetFileId, int targetRgId) throws RetinaException
    {
        Path path = offloadedCheckpoints.get(timestamp);
        if (path == null || !Files.exists(path))
        {
            throw new RetinaException("Checkpoint missing: " + timestamp);
        }

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path))))
        {
            int rgCount = in.readInt();
            for (int i = 0; i < rgCount; i++)
            {
                long fileId = in.readLong();
                int rgId = in.readInt();
                int len = in.readInt();
                if (fileId == targetFileId && rgId == targetRgId)
                {
                    long[] bitmap = new long[len];
                    for (int j = 0; j < len; j++)
                    {
                        bitmap[j] = in.readLong();
                    }
                    return bitmap;
                } else
                {
                    int skipped = in.skipBytes(len * 8);
                    if (skipped != len * 8)
                    {
                        throw new IOException("Unexpected EOF");
                    }
                }
            }
        } catch (IOException e)
        {
            throw new RetinaException("Failed to read checkpoint file", e);
        }
        return new long[0];
    }

    private void removeCheckpointFile(long timestamp, CheckpointType type)
    {
        String prefix = (type == CheckpointType.GC) ? "vis_gc_" : "vis_offload_";
        try
        {
            Files.deleteIfExists(checkpointDir.resolve(prefix + timestamp + ".bin"));
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

            PixelsWriteBuffer pixelsWriteBuffer = new PixelsWriteBuffer(latestLayout.getTableId(),
                    schema, orderedPaths.get(0), compactPaths.get(0), retinaHostName);
            String writeBufferKey = schemaName + "_" + tableName;
            pixelsWriteBufferMap.put(writeBufferKey, pixelsWriteBuffer);
        } catch (Exception e)
        {
            throw new RetinaException(String.format("Failed to add writer buffer for schema %s, table %s", schemaName, tableName), e);
        }
    }

    public IndexProto.PrimaryIndexEntry.Builder insertRecord(String schemaName, String tableName, byte[][] colValues, long timestamp) throws RetinaException
    {
        IndexProto.PrimaryIndexEntry.Builder builder = IndexProto.PrimaryIndexEntry.newBuilder();
        PixelsWriteBuffer writeBuffer = checkPixelsWriteBuffer(schemaName, tableName);
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

    public RetinaProto.GetWriteBufferResponse.Builder getWriteBuffer(String schemaName, String tableName, long timestamp) throws RetinaException
    {
        RetinaProto.GetWriteBufferResponse.Builder responseBuilder = RetinaProto.GetWriteBufferResponse.newBuilder();

        // get super version
        PixelsWriteBuffer writeBuffer = checkPixelsWriteBuffer(schemaName, tableName);
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
    private PixelsWriteBuffer checkPixelsWriteBuffer(String schema, String table) throws RetinaException
    {
        String writeBufferKey = schema + "_" + table;
        PixelsWriteBuffer writeBuffer = this.pixelsWriteBufferMap.get(writeBufferKey);
        if (writeBuffer == null)
        {
            throw new RetinaException(String.format("Writer buffer not found for schema: %s, table: %s", schema, table));
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
            createDiskCheckpoint(timestamp, CheckpointType.GC);
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
