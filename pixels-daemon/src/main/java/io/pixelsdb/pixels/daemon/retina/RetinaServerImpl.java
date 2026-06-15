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
package io.pixelsdb.pixels.daemon.retina;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Striped;
import com.google.protobuf.ByteString;
import com.sun.management.OperatingSystemMXBean;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.ResolvedPrimary;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.index.service.IndexServiceProvider;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.IndexUtils;
import io.pixelsdb.pixels.daemon.heartbeat.HeartbeatWorker;
import io.pixelsdb.pixels.daemon.heartbeat.NodeStatus;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RGVisibility;
import io.pixelsdb.pixels.retina.RecoveryCheckpoint;
import io.pixelsdb.pixels.retina.RecoveryCheckpoint.Body;
import io.pixelsdb.pixels.retina.RecoveryCheckpoint.LoadedCheckpoint;
import io.pixelsdb.pixels.retina.RecoveryCheckpoint.PendingSegmentEntry;
import io.pixelsdb.pixels.retina.RecoveryCheckpoint.VisibilityEntry;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.retina.RetinaProto.RetinaState;
import io.pixelsdb.pixels.retina.RetinaResourceManager;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import io.pixelsdb.pixels.retina.StorageGcWal;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static java.util.Objects.requireNonNull;

/**
 * Created at: 24-12-20
 * Author: gengdy
 */
public class RetinaServerImpl extends RetinaWorkerServiceGrpc.RetinaWorkerServiceImplBase
{
    private static final Logger logger = LogManager.getLogger(RetinaServerImpl.class);
    private final MetadataService metadataService;
    private final IndexService indexService;
    private final RetinaResourceManager retinaResourceManager;
    private final Striped<Lock> updateLocks = Striped.lock(1024);
    private volatile RetinaStatus status;
    private IndexOption[] indexOptionPool;

    /**
     * Initialize the visibility management for all the records.
     */
    public RetinaServerImpl()
    {
        this(MetadataService.Instance(),
                IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.local),
                RetinaResourceManager.Instance());
    }

    RetinaServerImpl(MetadataService metadataService, IndexService indexService,
                     RetinaResourceManager retinaResourceManager)
    {
        this.metadataService = requireNonNull(metadataService, "metadataService is null");
        this.indexService = requireNonNull(indexService, "indexService is null");
        this.retinaResourceManager = requireNonNull(retinaResourceManager, "retinaResourceManager is null");

        int totalBuckets = Integer.parseInt(ConfigFactory.Instance().getProperty("index.bucket.num"));
        this.indexOptionPool = new IndexOption[totalBuckets];
        for (int i = 0; i < totalBuckets; i++)
        {
            this.indexOptionPool[i] = new IndexOption();
            this.indexOptionPool[i].setVNodeId(i);
        }

        try
        {
            RecoveryContext recoveryContext = prepareRecoveryContext();
            RecoveryResult recoveryResult = recoverRetinaState(recoveryContext);
            initializeRecoveredResources();
            publishStartupLifecycle(recoveryContext, recoveryResult);
            startRetinaMetricsLogThread();
            boolean ready = this.status.getState() == RetinaState.READY;
            logger.info(ready ? "Retina service is ready" : "Retina service is recovering");
        }
        catch (Exception e)
        {
            RetinaStatus base = this.status;
            RetinaStatus.Builder builder = base == null
                    ? RetinaStatus.newBuilder()
                    : base.toBuilder();
            this.status = builder
                    .setState(RetinaState.FAILED)
                    .build();
            this.retinaResourceManager.setRecovering(false);
            HeartbeatWorker.setCurrentStatus(NodeStatus.INIT);
            logger.error("Error while initializing RetinaServerImpl", e);
            throw new IllegalStateException("Failed to initialize RetinaServerImpl", e);
        }
    }

    private RecoveryContext prepareRecoveryContext() throws RetinaException
    {
        String recoveryEpoch = UUID.randomUUID().toString();

        RecoveryCheckpoint recoveryCheckpoint = RecoveryCheckpoint.createFromConfig();
        int virtualNodesPerNode = recoveryCheckpoint.getVirtualNodesPerNode();
        if (virtualNodesPerNode <= 0)
        {
            throw new RetinaException("virtualNodesPerNode must be positive, got " + virtualNodesPerNode);
        }
        // Config only provides vnode count; expected vnode ids are derived as [0, N).
        Set<Integer> expectedVnodes = new HashSet<>(virtualNodesPerNode);
        for (int i = 0; i < virtualNodesPerNode; i++)
        {
            expectedVnodes.add(i);
        }

        StorageGcWal storageGcWal = retinaResourceManager.getStorageGcWal();
        StorageGcWal.RecoveryHandler storageGcWalRecoveryHandler = new StorageGcWal.RecoveryHandler(
                storageGcWal, metadataService, indexService);

        return new RecoveryContext(
                recoveryEpoch,
                storageGcWal,
                storageGcWalRecoveryHandler,
                recoveryCheckpoint.load(),
                expectedVnodes);
    }

    private RecoveryResult recoverRetinaState(RecoveryContext context) throws RetinaException
    {
        LoadedCheckpoint loaded = context.loadedCheckpoint;
        if (loaded == null)
        {
            context.storageGcWalRecoveryHandler.recover(Collections.emptySet());
            try
            {
                if (!metadataService.getFilesByType(EnumSet.of(File.Type.REGULAR)).isEmpty())
                {
                    throw new RetinaException("Recovery aborted: no checkpoint body found but catalog has REGULAR files");
                }
            }
            catch (MetadataException e)
            {
                throw new RetinaException("Recovery catalog probe failed", e);
            }
            return new RecoveryResult(true, computeReplay(0L, Collections.emptyList(), context.expectedVnodes));
        }

        Body body = loaded.body;
        logger.info("Recovery: applying checkpoint body={} (checkpointAppliedTs={})",
                loaded.bodyPath, body.getCheckpointAppliedTs());

        int warnCount = 0;
        Map<Long, File> catalogByFileId = new HashMap<>();
        Set<Long> droppedFileIds = new HashSet<>();
        List<VisibilityEntry> validRgEntries = new ArrayList<>(body.getRgEntries().size());
        Set<Long> recoverableFileIds = new HashSet<>();

        for (VisibilityEntry ve : body.getRgEntries())
        {
            long fileId = ve.getFileId();
            if (droppedFileIds.contains(fileId))
            {
                warnCount++;
                continue;
            }
            File catalog = catalogByFileId.get(fileId);
            if (catalog == null)
            {
                try
                {
                    catalog = metadataService.getFileById(fileId);
                }
                catch (MetadataException e)
                {
                    throw new RetinaException("Recovery cleanser: catalog lookup failed for fileId=" + fileId, e);
                }
                if (catalog == null)
                {
                    logger.warn("Recovery cleanser: dropping VisibilityEntry fileId={}, rgId={} because fileId is not in catalog",
                            fileId, ve.getRgId());
                    droppedFileIds.add(fileId);
                    warnCount++;
                    continue;
                }
                if (catalog.getType() != File.Type.REGULAR)
                {
                    logger.warn("Recovery cleanser: dropping VisibilityEntry fileId={}, rgId={} because catalog type is {}",
                            fileId, ve.getRgId(), catalog.getType());
                    droppedFileIds.add(fileId);
                    warnCount++;
                    continue;
                }
                if (catalog.getMinRowId() < 0 || catalog.getMaxRowId() < catalog.getMinRowId())
                {
                    logger.warn("Recovery cleanser: dropping VisibilityEntry fileId={}, rgId={} because catalog hull is invalid: min={}, max={}",
                            fileId, ve.getRgId(), catalog.getMinRowId(), catalog.getMaxRowId());
                    droppedFileIds.add(fileId);
                    warnCount++;
                    continue;
                }
                catalogByFileId.put(fileId, catalog);
            }
            if (ve.getRgId() < 0 || ve.getRecordNum() <= 0 || catalog.getNumRowGroup() <= ve.getRgId())
            {
                logger.warn("Recovery cleanser: dropping VisibilityEntry fileId={}, rgId={}, recordNum={}, catalogRowGroups={}",
                        fileId, ve.getRgId(), ve.getRecordNum(), catalog.getNumRowGroup());
                warnCount++;
                continue;
            }
            validRgEntries.add(ve);
            recoverableFileIds.add(fileId);
        }

        context.storageGcWalRecoveryHandler.recover(recoverableFileIds);

        for (VisibilityEntry ve : validRgEntries)
        {
            retinaResourceManager.addVisibility(ve.getFileId(), ve.getRgId(),
                    ve.getRecordNum(), ve.getBaseTimestamp(), ve.getBitmap(), true);
        }

        ReplayResult replay = computeReplay(
                body.getCheckpointAppliedTs(),
                body.getSegmentEntries(),
                context.expectedVnodes);

        long cleanupAt = System.currentTimeMillis();
        Set<Long> pendingJournalFileIds;
        try
        {
            pendingJournalFileIds = context.storageGcWal.collectPendingFileIds();
        }
        catch (RuntimeException e)
        {
            throw new RetinaException("Recovery retirer: failed to load Storage GC journal tasks", e);
        }

        List<File> catalogRegulars;
        try
        {
            catalogRegulars = metadataService.getFilesByType(EnumSet.of(File.Type.REGULAR));
        }
        catch (MetadataException e)
        {
            throw new RetinaException("Recovery retirer: catalog-wide REGULAR scan failed", e);
        }

        int retiredCount = 0;
        int protectedCount = 0;
        for (File catalog : catalogRegulars)
        {
            long fileId = catalog.getId();
            if (recoverableFileIds.contains(fileId))
            {
                continue;
            }
            if (pendingJournalFileIds.contains(fileId))
            {
                protectedCount++;
                continue;
            }
            catalog.setType(File.Type.RETIRED);
            catalog.setCleanupAt(cleanupAt);
            try
            {
                if (!metadataService.updateFile(catalog))
                {
                    throw new RetinaException("Recovery retirer: updateFile returned false for fileId=" + fileId);
                }
                retiredCount++;
            }
            catch (MetadataException e)
            {
                throw new RetinaException("Recovery retirer: failed to retire fileId=" + fileId, e);
            }
        }

        logger.info("Recovery complete: checkpointId={}, checkpointAppliedTs={}, baselineFiles={}, retired={}, protected={}, nodeReplayFromTs={}, warns={}",
                loaded.bodyPath,
                body.getCheckpointAppliedTs(),
                recoverableFileIds.size(),
                retiredCount,
                protectedCount,
                replay.nodeReplayFromTs,
                warnCount);

        // Clean up terminal tasks related to this checkpoint to prevent blocking future recoveries
        try
        {
            List<StorageGcWal.Task> terminalTasks = context.storageGcWal.listTerminalTasks();
            if (!terminalTasks.isEmpty())
            {
                List<String> tasksToDelete = new ArrayList<>(terminalTasks.size());
                for (StorageGcWal.Task task : terminalTasks)
                {
                    tasksToDelete.add(task.getTaskId());
                }
                context.storageGcWal.deleteTerminalTasks(tasksToDelete);
                logger.info("Cleaned up {} terminal Storage GC WAL tasks after checkpoint ts={}",
                        tasksToDelete.size(), body.getCheckpointAppliedTs());
            }
        }
        catch (Exception e)
        {
            logger.warn("Failed to cleanup terminal Storage GC journal tasks: {}", e.getMessage());
        }

        return new RecoveryResult(false, replay);
    }

    private void initializeRecoveredResources() throws Exception
    {
        this.retinaResourceManager.recoverOffloadCheckpoints();
        List<Schema> schemas = this.metadataService.getSchemas();
        for (Schema schema : schemas)
        {
            List<Table> tables = this.metadataService.getTables(schema.getName());
            for (Table table : tables)
            {
                this.retinaResourceManager.addWriteBuffer(schema.getName(), table.getName());
            }
        }
    }

    private void publishStartupLifecycle(RecoveryContext context, RecoveryResult result) throws RetinaException
    {
        this.retinaResourceManager.setRecovering(true);
        HeartbeatWorker.setCurrentStatus(NodeStatus.INIT);
        this.status = RetinaStatus.newBuilder()
                .setState(RetinaState.RECOVERING)
                .setRecoveryEpoch(context.recoveryEpoch)
                .putAllVnodeReplayStarts(result.replay.vnodeReplayStarts)
                .build();

        if (result.freshDeployment)
        {
            retinaResourceManager.startBackgroundGc();
            this.status = this.status.toBuilder()
                    .setState(RetinaState.READY)
                    .build();
            this.retinaResourceManager.setRecovering(false);
            HeartbeatWorker.setCurrentStatus(NodeStatus.READY);
        }
    }

    private static final class RecoveryContext
    {
        final String recoveryEpoch;
        final StorageGcWal storageGcWal;
        final StorageGcWal.RecoveryHandler storageGcWalRecoveryHandler;
        final LoadedCheckpoint loadedCheckpoint;
        final Set<Integer> expectedVnodes;

        RecoveryContext(String recoveryEpoch,
                        StorageGcWal storageGcWal,
                        StorageGcWal.RecoveryHandler storageGcWalRecoveryHandler,
                        LoadedCheckpoint loadedCheckpoint,
                        Set<Integer> expectedVnodes)
        {
            this.recoveryEpoch = recoveryEpoch;
            this.storageGcWal = storageGcWal;
            this.storageGcWalRecoveryHandler = storageGcWalRecoveryHandler;
            this.loadedCheckpoint = loadedCheckpoint;
            this.expectedVnodes = expectedVnodes;
        }
    }

    private static final class RecoveryResult
    {
        final boolean freshDeployment;
        final ReplayResult replay;

        RecoveryResult(boolean freshDeployment, ReplayResult replay)
        {
            this.freshDeployment = freshDeployment;
            this.replay = replay;
        }
    }

    private static final class ReplayResult
    {
        final Map<Integer, Long> vnodeReplayStarts;
        final long nodeReplayFromTs;

        ReplayResult(Map<Integer, Long> vnodeReplayStarts, long nodeReplayFromTs)
        {
            this.vnodeReplayStarts = Collections.unmodifiableMap(new HashMap<>(vnodeReplayStarts));
            this.nodeReplayFromTs = nodeReplayFromTs;
        }
    }

    private static ReplayResult computeReplay(long checkpointAppliedTs,
                                              List<PendingSegmentEntry> segmentEntries,
                                              Set<Integer> expectedVnodes)
    {
        Map<Integer, Long> vnodeReplayStarts = new HashMap<>();
        for (PendingSegmentEntry se : segmentEntries)
        {
            long ts = se.getMinCommitTs() == Long.MAX_VALUE
                    ? checkpointAppliedTs
                    : Math.min(checkpointAppliedTs, se.getMinCommitTs());
            vnodeReplayStarts.merge(se.getVirtualNodeId(), ts, Math::min);
        }
        for (Integer vnode : expectedVnodes)
        {
            vnodeReplayStarts.putIfAbsent(vnode, checkpointAppliedTs);
        }
        long nodeReplayFromTs = checkpointAppliedTs;
        if (!vnodeReplayStarts.isEmpty())
        {
            nodeReplayFromTs = Long.MAX_VALUE;
            for (Long v : vnodeReplayStarts.values())
            {
                nodeReplayFromTs = Math.min(nodeReplayFromTs, v);
            }
        }
        return new ReplayResult(vnodeReplayStarts, nodeReplayFromTs);
    }

    private static String getRetinaMetrics(OperatingSystemMXBean osBean)
    {
        /* Get basic runtime and resource info */
        Runtime runtime = Runtime.getRuntime();
        int availableProcessors = runtime.availableProcessors();
        double GiB = 1024.0 * 1024.0 * 1024.0;
        long timestamp = System.currentTimeMillis();

        /* Collect Retina Native Metrics */
        long nativeAllocated = 0;
        long trackedMem = 0;
        long objectCount = 0;
        try
        {
            nativeAllocated = RGVisibility.getMemoryUsage();
            trackedMem = RGVisibility.getTrackedMemoryUsage();
            objectCount = RGVisibility.getRetinaTrackedObjectCount();
        }
        catch (Exception e)
        {
            logger.warn("Failed to retrieve Retina native metrics: {}", e.getMessage());
        }

        /* Calculate memory minus the monitoring overhead (vptr) */
        long vptrOverhead = objectCount * 8;
        long pureTracked = Math.max(0, trackedMem - vptrOverhead);

        /* Collect JVM Heap Metrics */
        long heapCommitted = runtime.totalMemory();
        long heapUsed = heapCommitted - runtime.freeMemory();
        long heapMax = runtime.maxMemory();

        double rawProcessLoad = osBean.getProcessCpuLoad();
        double rawSystemLoad = osBean.getSystemCpuLoad();

        /* Handle cases where the bean is not yet initialized (-1.0) */
        double processLoadVal = (rawProcessLoad < 0) ? 0.0 : rawProcessLoad;
        double systemLoadVal = (rawSystemLoad < 0) ? 0.0 : rawSystemLoad;

        /* Calculate percentage consistent with 'top' command (100% per core) */
        double processCpuPercentage = processLoadVal * availableProcessors * 100.0;
        double systemCpuPercentage = systemLoadVal * 100.0;

        /* Format final log output */
        return String.format(
                "Timestamp=%d CPU_Usage[Process: %.4f%%, System: %.4f%%] " +
                        "Retina_Mem[Allocated: %.4f GiB (%d Bytes), Tracked: %d Bytes, Objects: %d, Pure_Tracked: %d Bytes] " +
                        "JVM_Heap[Used: %.4f GiB, Committed: %.4f GiB, Max: %.4f GiB]",
                timestamp,
                processCpuPercentage,
                systemCpuPercentage,
                nativeAllocated / GiB, nativeAllocated,
                trackedMem,
                objectCount,
                pureTracked,
                heapUsed / GiB,
                heapCommitted / GiB,
                heapMax / GiB
        );
    }

    @Override
    public void updateRecord(RetinaProto.UpdateRecordRequest request,
                             StreamObserver<RetinaProto.UpdateRecordResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try
        {
            processUpdateRequest(request);
            responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
        }
        catch (RetinaException e)
        {
            logger.error("updateRecord failed for schema={} (retina)", request.getSchemaName(), e);
            headerBuilder.setErrorCode(ErrorCode.RETINA_UPDATE_FAILED).setErrorMsg("Retina: " + e.getMessage());
            responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
        }
        catch (IndexException e)
        {
            logger.error("updateRecord failed for schema={} (index)", request.getSchemaName(), e);
            headerBuilder.setErrorCode(ErrorCode.RETINA_UPDATE_FAILED).setErrorMsg("Index: " + e.getMessage());
            responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
        }
        finally
        {
            responseObserver.onCompleted();
        }
    }

    @Override
    public StreamObserver<RetinaProto.UpdateRecordRequest> streamUpdateRecord(
            StreamObserver<RetinaProto.UpdateRecordResponse> responseObserver)
    {
        return new StreamObserver<RetinaProto.UpdateRecordRequest>()
        {
            @Override
            public void onNext(RetinaProto.UpdateRecordRequest request)
            {
                RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                        .setToken(request.getHeader().getToken());

                try
                {
                    processUpdateRequest(request);
                    responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                            .setHeader(headerBuilder.build())
                            .build());
                }
                catch (RetinaException e)
                {
                    headerBuilder.setErrorCode(ErrorCode.RETINA_UPDATE_FAILED).setErrorMsg("Retina: " + e.getMessage());
                    responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                            .setHeader(headerBuilder.build())
                            .build());
                    logger.error("Error processing streaming update (retina)", e);
                }
                catch (IndexException e)
                {
                    headerBuilder.setErrorCode(ErrorCode.RETINA_UPDATE_FAILED).setErrorMsg("Index: " + e.getMessage());
                    responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                            .setHeader(headerBuilder.build())
                            .build());
                    logger.error("Error processing streaming update (index)", e);
                }
                catch (Exception e)
                {
                    headerBuilder.setErrorCode(ErrorCode.RETINA_UPDATE_FAILED).setErrorMsg("Internal error: " + e.getMessage());
                    responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                            .setHeader(headerBuilder.build())
                            .build());
                    logger.error("Unexpected error processing streaming update", e);
                }
                catch (Throwable t)
                {
                    headerBuilder.setErrorCode(ErrorCode.RETINA_UPDATE_FAILED).setErrorMsg("Fatal error: " + t.getMessage());
                    responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                            .setHeader(headerBuilder.build())
                            .build());
                    logger.error("Fatal error processing streaming update", t);
                }
            }

            @Override
            public void onError(Throwable t)
            {
                logger.error("streamUpdateRecord failed", t);
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted()
            {
                responseObserver.onCompleted();
            }
        };
    }

    /**
     * Transpose the index keys from a row set to a column set.
     *
     * @param dataList
     * @param indexExtractor
     * @param <T>
     * @return
     */
    private <T> List<List<IndexProto.IndexKey>> transposeIndexKeys(List<T> dataList,
                                                                   Function<T, List<IndexProto.IndexKey>> indexExtractor)
    {
        if (dataList == null || dataList.isEmpty())
        {
            return Collections.emptyList();
        }

        return new TransposedIndexKeyView<>(dataList, indexExtractor);
    }

    /**
     * Common template to group data by Index Bucket and process in parallel.
     * * @param dataList The source list (DeleteData, InsertData, or UpdateData).
     *
     * @param keyExtractor Function to get the IndexKey from the data object.
     * @param processor    The lambda containing the specific business logic for each bucket.
     */
    private <T> void executeParallelByBucket(
            List<T> dataList,
            java.util.function.Function<T, IndexProto.IndexKey> keyExtractor,
            BucketProcessor<T> processor) throws RetinaException, IndexException
    {
        if (dataList == null || dataList.isEmpty())
        {
            return;
        }

        // 1. Grouping: Calculate bucketId once per record and group them
        Map<Integer, List<T>> bucketMap = dataList.stream()
                .collect(Collectors.groupingBy(d ->
                        IndexUtils.getBucketIdFromByteBuffer(keyExtractor.apply(d).getKey())));

                        // 2. Parallel Execution: Process each bucket in parallel
        // This utilizes the common ForkJoinPool to execute RPCs and logic simultaneously
        try
        {
            bucketMap.entrySet().parallelStream().forEach(entry ->
            {
                int bucketId = entry.getKey();
                List<T> subList = entry.getValue();

            // Fetch the pre-initialized IndexOption from the pool (Zero allocation)
                IndexOption option = this.indexOptionPool[bucketId];

                try
                {
                // Execute the specific Delete/Insert/Update logic
                    processor.process(bucketId, subList, option);
                }
                catch (Exception e)
                {
                // Wrap checked exceptions to propagate through the parallel stream
                    throw new RuntimeException("Failure during parallel index processing for Bucket: " + bucketId, e);
                }
            });
        }
        catch (RuntimeException e)
        {
            Throwable cause = e;
            while (cause instanceof RuntimeException && cause.getCause() != null)
            {
                cause = cause.getCause();
            }
            if (cause instanceof RetinaException)
            {
                throw (RetinaException) cause;
            }
            if (cause instanceof IndexException)
            {
                throw (IndexException) cause;
            }
            throw e;
        }
    }

    /**
     * Generic method to process secondary indexes for Insert and Update operations.
     */
    private <T> void processSecondaryIndexes(
            List<T> subList,
            Function<T, List<IndexProto.IndexKey>> keyListExtractor,
            List<Long> rowIdList,
            IndexOption indexOption,
            boolean isUpdate) throws IndexException
    {
        List<List<IndexProto.IndexKey>> indexKeysList = transposeIndexKeys(subList, keyListExtractor);
        // Start from i=1 because i=0 is always the Primary Index
        for (int i = 1; i < indexKeysList.size(); ++i)
        {
            List<IndexProto.IndexKey> indexKeys = indexKeysList.get(i);
            List<IndexProto.SecondaryIndexEntry> secondaryIndexEntries =
                    IntStream.range(0, indexKeys.size())
                            .mapToObj(j -> IndexProto.SecondaryIndexEntry.newBuilder()
                                    .setRowId(rowIdList.get(j))
                                    .setIndexKey(indexKeys.get(j))
                                    .build())
                            .collect(Collectors.toList());

            if (isUpdate)
            {
                indexService.updateSecondaryIndexEntries(indexKeys.get(0).getTableId(),
                        indexKeys.get(0).getIndexId(), secondaryIndexEntries, indexOption);
            } else
            {
                indexService.putSecondaryIndexEntries(indexKeys.get(0).getTableId(),
                        indexKeys.get(0).getIndexId(), secondaryIndexEntries, indexOption);
            }
        }
    }

    /**
     * Delete phase for one bucket. Hide existing rows before removing primary entries;
     * secondary cleanup is best effort.
     */
    private <T> void executeStagedDeletePhase(
            List<T> subList,
            java.util.function.Function<T, List<IndexProto.IndexKey>> keyListExtractor,
            long primaryIndexId, long timestamp, IndexOption option) throws IndexException, RetinaException
    {
        List<List<IndexProto.IndexKey>> keysList = transposeIndexKeys(subList, keyListExtractor::apply);
        List<IndexProto.IndexKey> primaryKeys = keysList.get(0);
        long tableId = primaryKeys.get(0).getTableId();

        List<Optional<ResolvedPrimary>> resolved =
                indexService.resolvePrimary(tableId, primaryIndexId, primaryKeys, option);
        List<IndexProto.IndexKey> foundKeys = new ArrayList<>(primaryKeys.size());
        for (int i = 0; i < primaryKeys.size(); i++)
        {
            Optional<ResolvedPrimary> r = resolved.get(i);
            if (r.isPresent())
            {
                this.retinaResourceManager.deleteRecord(r.get().getRowLocation(), timestamp);
                foundKeys.add(primaryKeys.get(i));
            }
            // Missing primary keys are no-op deletes.
        }
        if (!foundKeys.isEmpty())
        {
            indexService.deletePrimaryIndexEntriesOnly(tableId, primaryIndexId, foundKeys, option);
        }

        for (int i = 1; i < keysList.size(); ++i)
        {
            try
            {
                indexService.deleteSecondaryIndexEntries(tableId,
                        keysList.get(i).get(0).getIndexId(), keysList.get(i), option);
            }
            catch (IndexException e)
            {
                logger.warn("Best-effort staged secondary delete failed for tableId={}, indexId={}",
                        tableId, keysList.get(i).get(0).getIndexId(), e);
            }
        }
    }

    /**
     * Insert phase for one bucket. Write main index entries before primary entries
     * so new primary mappings point to resolvable row locations.
     */
    private <T> void executeStagedInsertPhase(
            String schemaName, String tableName, int virtualNodeId,
            List<T> subList,
            java.util.function.Function<T, List<IndexProto.IndexKey>> keyListExtractor,
            java.util.function.Function<T, List<ByteString>> colValuesExtractor,
            long primaryIndexId, long timestamp, IndexOption option) throws Exception
    {
        List<IndexProto.PrimaryIndexEntry> primaryEntries = new ArrayList<>(subList.size());
        List<Long> rowIds = new ArrayList<>(subList.size());
        List<IndexProto.RowLocation> insertedLocations = new ArrayList<>(subList.size());

        try
        {
            for (T data : subList)
            {
                byte[][] values = colValuesExtractor.apply(data).stream()
                        .map(ByteString::toByteArray).toArray(byte[][]::new);
                IndexProto.PrimaryIndexEntry.Builder builder = retinaResourceManager.insertRecord(
                        schemaName, tableName, values, timestamp, virtualNodeId);
                builder.setIndexKey(keyListExtractor.apply(data).get(0));
                IndexProto.PrimaryIndexEntry entry = builder.build();
                primaryEntries.add(entry);
                rowIds.add(entry.getRowId());
                insertedLocations.add(entry.getRowLocation());
            }

            long tableId = primaryEntries.get(0).getIndexKey().getTableId();
            indexService.putMainIndexEntriesOnly(tableId, primaryEntries);
            indexService.putPrimaryIndexEntriesOnly(tableId, primaryIndexId, primaryEntries, option);

            processSecondaryIndexes(subList, keyListExtractor::apply, rowIds, option, false);
        }
        catch (Exception e)
        {
            for (IndexProto.RowLocation loc : insertedLocations)
            {
                try
                {
                    this.retinaResourceManager.deleteRecord(loc, timestamp);
                }
                catch (Exception rollbackEx)
                {
                    logger.error("Failed to roll back visibility for inserted row at fileId={}, rgId={}, rgRowOffset={}",
                            loc.getFileId(), loc.getRgId(), loc.getRgRowOffset(), rollbackEx);
                }
            }
            throw e;
        }
    }

    /**
     * Update phase for one bucket. Resolve current rows, append replacements,
     * write main index entries, switch primary entries, then hide old rows.
     */
    private <T> void executeStagedUpdatePhase(
            String schemaName, String tableName, int virtualNodeId,
            int bucketId,
            List<T> subList,
            java.util.function.Function<T, List<IndexProto.IndexKey>> keyListExtractor,
            java.util.function.Function<T, List<ByteString>> colValuesExtractor,
            long primaryIndexId, long timestamp, IndexOption option) throws Exception
    {
        List<IndexProto.PrimaryIndexEntry> primaryEntries = new ArrayList<>(subList.size());
        List<Long> rowIds = new ArrayList<>(subList.size());
        List<IndexProto.RowLocation> insertedLocations = new ArrayList<>(subList.size());
        String lockKey = "v_" + virtualNodeId + "_b_" + bucketId + "_i_" + primaryIndexId;
        Lock lock = updateLocks.get(lockKey);

        try
        {
            lock.lock();
            try
            {
                List<List<IndexProto.IndexKey>> keysList = transposeIndexKeys(subList, keyListExtractor::apply);
                List<IndexProto.IndexKey> primaryKeys = keysList.get(0);
                long tableId = primaryKeys.get(0).getTableId();

                List<Optional<ResolvedPrimary>> resolved =
                        indexService.resolvePrimary(tableId, primaryIndexId, primaryKeys, option);
                if (resolved.size() != primaryKeys.size())
                {
                    throw new IndexException("Resolved primary count mismatch for tableId="
                            + tableId + ", indexId=" + primaryIndexId);
                }

                List<IndexProto.RowLocation> previousLocations = new ArrayList<>(primaryKeys.size());
                for (int i = 0; i < primaryKeys.size(); i++)
                {
                    Optional<ResolvedPrimary> r = resolved.get(i);
                    if (!r.isPresent())
                    {
                        throw new IndexException("Primary index entry not found for update, tableId="
                                + tableId + ", indexId=" + primaryIndexId);
                    }
                    previousLocations.add(r.get().getRowLocation());
                }

                for (T data : subList)
                {
                    byte[][] values = colValuesExtractor.apply(data).stream()
                            .map(ByteString::toByteArray).toArray(byte[][]::new);
                    IndexProto.PrimaryIndexEntry.Builder builder = retinaResourceManager.insertRecord(
                            schemaName, tableName, values, timestamp, virtualNodeId);
                    builder.setIndexKey(keyListExtractor.apply(data).get(0));
                    IndexProto.PrimaryIndexEntry entry = builder.build();
                    primaryEntries.add(entry);
                    rowIds.add(entry.getRowId());
                    insertedLocations.add(entry.getRowLocation());
                }

                // TODO: replace this JVM-local lock with an index API that updates only when the
                // resolved old rowIds still match, so concurrent writers can avoid bucket serialization.
                indexService.putMainIndexEntriesOnly(tableId, primaryEntries);
                indexService.updatePrimaryIndexEntriesOnly(tableId, primaryIndexId, primaryEntries, option);
                for (IndexProto.RowLocation loc : previousLocations)
                {
                    this.retinaResourceManager.deleteRecord(loc, timestamp);
                }
            }
            finally
            {
                lock.unlock();
            }

            processSecondaryIndexes(subList, keyListExtractor::apply, rowIds, option, true);
        }
        catch (Exception e)
        {
            for (IndexProto.RowLocation loc : insertedLocations)
            {
                try
                {
                    this.retinaResourceManager.deleteRecord(loc, timestamp);
                }
                catch (Exception rollbackEx)
                {
                    logger.error("Failed to roll back visibility for inserted row at fileId={}, rgId={}, rgRowOffset={}",
                            loc.getFileId(), loc.getRgId(), loc.getRgRowOffset(), rollbackEx);
                }
            }
            throw e;
        }
    }

    /**
     * Common method to process updates for both normal and streaming rpc.
     *
     * @param request
     * @throws RetinaException
     * @throws IndexException
     */
    private void processUpdateRequest(RetinaProto.UpdateRecordRequest request) throws RetinaException, IndexException
    {
        String schemaName = request.getSchemaName();
        List<RetinaProto.TableUpdateData> tableUpdateDataList = request.getTableUpdateDataList();
        int virtualNodeId = request.getVirtualNodeId();
        if (tableUpdateDataList.isEmpty())
        {
            return;
        }
        for (RetinaProto.TableUpdateData tableUpdateData : tableUpdateDataList)
        {
            String tableName = tableUpdateData.getTableName();
            long primaryIndexId = tableUpdateData.getPrimaryIndexId();
            long timestamp = tableUpdateData.getTimestamp();

            // =================================================================
            // 1. Process Delete Data
            // =================================================================
            List<RetinaProto.DeleteData> deleteDataList = tableUpdateData.getDeleteDataList();
            if (!deleteDataList.isEmpty())
            {
                validateIndexData(deleteDataList, d -> d.getIndexKeysList(), primaryIndexId, "Delete");

                executeParallelByBucket(deleteDataList, d -> d.getIndexKeys(0), (bucketId, subList, option) ->
                        executeStagedDeletePhase(subList, RetinaProto.DeleteData::getIndexKeysList,
                                primaryIndexId, timestamp, option));
            }

            // =================================================================
            // 2. Process Insert Data
            // =================================================================
            List<RetinaProto.InsertData> insertDataList = tableUpdateData.getInsertDataList();
            if (!insertDataList.isEmpty())
            {
                validateIndexData(insertDataList, d -> d.getIndexKeysList(), primaryIndexId, "Insert");

                executeParallelByBucket(insertDataList, d -> d.getIndexKeys(0), (bucketId, subList, option) ->
                        executeStagedInsertPhase(schemaName, tableName, virtualNodeId, subList,
                                RetinaProto.InsertData::getIndexKeysList,
                                RetinaProto.InsertData::getColValuesList,
                                primaryIndexId, timestamp, option));
            }
            // =================================================================
            // 3. Process Update Data
            //
            // UpdateData keeps primary-index update semantics; new row locations
            // are written before primary entries are switched.
            // =================================================================
            List<RetinaProto.UpdateData> updateDataList = tableUpdateData.getUpdateDataList();
            if (!updateDataList.isEmpty())
            {
                validateIndexData(updateDataList, d -> d.getIndexKeysList(), primaryIndexId, "Update");

                executeParallelByBucket(updateDataList, d -> d.getIndexKeys(0), (bucketId, subList, option) ->
                        executeStagedUpdatePhase(schemaName, tableName, virtualNodeId, bucketId, subList,
                                RetinaProto.UpdateData::getIndexKeysList,
                                RetinaProto.UpdateData::getColValuesList,
                                primaryIndexId, timestamp, option));
            }
        }
    }

    /**
     * Shared validation logic for index data.
     */
    private <T> void validateIndexData(List<T> dataList, java.util.function.Function<T, List<IndexProto.IndexKey>> keyListExtractor, long primaryIndexId, String opType) throws RetinaException
    {
        int indexNum = keyListExtractor.apply(dataList.get(0)).size();
        if (indexNum == 0)
            throw new RetinaException(opType + " index key list is empty");

        boolean valid = dataList.stream().allMatch(d ->
        {
            List<IndexProto.IndexKey> keys = keyListExtractor.apply(d);
            return keys.size() == indexNum && keys.get(0).getIndexId() == primaryIndexId;
        });
        if (!valid)
            throw new RetinaException("Primary index id mismatch or inconsistent index key list size in " + opType);
    }

    @Override
    public void getRetinaStatus(RetinaProto.GetRetinaStatusRequest request,
                                StreamObserver<RetinaProto.GetRetinaStatusResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        RetinaProto.GetRetinaStatusResponse.Builder response =
                RetinaProto.GetRetinaStatusResponse.newBuilder().setHeader(headerBuilder);
        RetinaStatus current = this.status;
        response.setState(current.getState())
                .setRecoveryEpoch(current.getRecoveryEpoch());
        // vnodeReplayStarts is only meaningful while RECOVERING; CDC drives
        // replay from those timestamps. Once published, the plan is ready by
        // construction, so no separate readiness flag is needed.
        if (current.getState() == RetinaState.RECOVERING)
        {
            current.getVnodeReplayStartsMap().entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> response.addVnodeReplayStarts(RetinaProto.VnodeReplayStart.newBuilder()
                            .setVirtualNodeId(entry.getKey())
                            .setStartTs(entry.getValue())
                            .build()));
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void markReady(RetinaProto.MarkReadyRequest request,
                          StreamObserver<RetinaProto.MarkReadyResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        try
        {
            RetinaStatus current = this.status;
            if (!current.getRecoveryEpoch().equals(request.getRecoveryEpoch()))
            {
                throw new RetinaException("MarkReady recoveryEpoch mismatch");
            }
            if (current.getState() != RetinaState.RECOVERING && current.getState() != RetinaState.READY)
            {
                throw new RetinaException("Retina is " + current.getState() + "; cannot mark READY");
            }

            // Start GC before publishing READY so a failed scheduler start remains retryable.
            retinaResourceManager.startBackgroundGc();
            this.status = current.toBuilder()
                    .setState(RetinaState.READY)
                    .build();
            this.retinaResourceManager.setRecovering(false);
            HeartbeatWorker.setCurrentStatus(NodeStatus.READY);
        }
        catch (RetinaException e)
        {
            headerBuilder.setErrorCode(ErrorCode.RETINA_MARK_READY_FAILED).setErrorMsg(e.getMessage());
        }
        responseObserver.onNext(RetinaProto.MarkReadyResponse.newBuilder()
                .setHeader(headerBuilder.build())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void addVisibility(RetinaProto.AddVisibilityRequest request,
                              StreamObserver<RetinaProto.AddVisibilityResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try
        {
            String filePath = request.getFilePath();
            this.retinaResourceManager.addVisibility(filePath);

            RetinaProto.AddVisibilityResponse response = RetinaProto.AddVisibilityResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        catch (RetinaException e)
        {
            headerBuilder.setErrorCode(ErrorCode.RETINA_VISIBILITY_FAILED).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.AddVisibilityResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void queryVisibility(RetinaProto.QueryVisibilityRequest request,
                                StreamObserver<RetinaProto.QueryVisibilityResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try
        {
            long fileId = request.getFileId();
            int[] rgIds = request.getRgIdsList().stream().mapToInt(Integer::intValue).toArray();
            long timestamp = request.getTimestamp();
            long transId = request.hasTransId() ? request.getTransId() : -1;

            RetinaProto.QueryVisibilityResponse.Builder responseBuilder = RetinaProto.QueryVisibilityResponse
                    .newBuilder()
                    .setHeader(headerBuilder.build());

            String checkpointPath = this.retinaResourceManager.getOffloadCheckpointPath(timestamp);
            if (checkpointPath != null)
            {
                responseBuilder.setCheckpointPath(checkpointPath);
            }
            else
            {
                for (int rgId : rgIds)
                {
                    long[] visibilityBitmap = this.retinaResourceManager.queryVisibility(fileId, rgId, timestamp, transId);
                    RetinaProto.VisibilityBitmap bitmap = RetinaProto.VisibilityBitmap.newBuilder()
                            .addAllBitmap(Arrays.stream(visibilityBitmap).boxed().collect(Collectors.toList()))
                            .build();
                    responseBuilder.addBitmaps(bitmap);
                }
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }
        catch (RetinaException e)
        {
            headerBuilder.setErrorCode(ErrorCode.RETINA_VISIBILITY_FAILED).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.QueryVisibilityResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void reclaimVisibility(RetinaProto.ReclaimVisibilityRequest request,
                                  StreamObserver<RetinaProto.ReclaimVisibilityResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try
        {
            long fileId = request.getFileId();
            int[] rgIds = request.getRgIdsList().stream().mapToInt(Integer::intValue).toArray();
            long timestamp = request.getTimestamp();
            for (int rgId : rgIds)
            {
                this.retinaResourceManager.reclaimVisibility(fileId, rgId, timestamp);
            }

            RetinaProto.ReclaimVisibilityResponse response = RetinaProto.ReclaimVisibilityResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        catch (RetinaException e)
        {
            headerBuilder.setErrorCode(ErrorCode.RETINA_VISIBILITY_FAILED).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.ReclaimVisibilityResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void addWriteBuffer(RetinaProto.AddWriteBufferRequest request,
                               StreamObserver<RetinaProto.AddWriteBufferResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        try
        {
            this.retinaResourceManager.addWriteBuffer(request.getSchemaName(), request.getTableName());

            RetinaProto.AddWriteBufferResponse response = RetinaProto.AddWriteBufferResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        catch (RetinaException e)
        {
            headerBuilder.setErrorCode(ErrorCode.RETINA_NOT_READY).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.AddWriteBufferResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getWriteBuffer(RetinaProto.GetWriteBufferRequest request,
                               StreamObserver<RetinaProto.GetWriteBufferResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try
        {
            RetinaProto.GetWriteBufferResponse.Builder response = this.retinaResourceManager.getWriteBuffer(
                    request.getSchemaName(), request.getTableName(), request.getTimestamp(), request.getVirtualNodeId());
            response.setHeader(headerBuilder);

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }
        catch (RetinaException e)
        {
            headerBuilder.setErrorCode(ErrorCode.RETINA_NOT_READY).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.GetWriteBufferResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void registerOffload(RetinaProto.RegisterOffloadRequest request,
                                StreamObserver<RetinaProto.RegisterOffloadResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try
        {
            this.retinaResourceManager.registerOffload(request.getTimestamp());
            responseObserver.onNext(RetinaProto.RegisterOffloadResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build());
        }
        catch (RetinaException e)
        {
            logger.error("registerOffload failed for timestamp={}",
                    request.getTimestamp(), e);
            headerBuilder.setErrorCode(ErrorCode.RETINA_NOT_READY).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.RegisterOffloadResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build());
        }
        finally
        {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void unregisterOffload(RetinaProto.UnregisterOffloadRequest request,
                                  StreamObserver<RetinaProto.UnregisterOffloadResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try
        {
            this.retinaResourceManager.unregisterOffload(request.getTimestamp());
            responseObserver.onNext(RetinaProto.UnregisterOffloadResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build());
        }
        catch (Exception e)
        {
            logger.error("unregisterOffload failed for timestamp={}",
                    request.getTimestamp(), e);
            headerBuilder.setErrorCode(ErrorCode.RETINA_NOT_READY).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.UnregisterOffloadResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build());
        }
        finally
        {
            responseObserver.onCompleted();
        }
    }

    /**
     * Start a background thread to log Retina-specific metrics including CPU,
     * Native Memory (via jemalloc), and JVM Heap.
     */
    private void startRetinaMetricsLogThread()
    {
        // Read interval from config, default to 60 seconds to match MetricsServer style
        int logInterval = Integer.parseInt(
                ConfigFactory.Instance().getProperty("retina.metrics.log.interval")
        );

        if (logInterval <= 0)
        {
            logger.info("Retina metrics is disabled");
            return;
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable ->
        {
            Thread thread = new Thread(runnable, "RetinaMetricsLogger");
            thread.setDaemon(true);
            return thread;
        });

        // Cast to com.sun.management.OperatingSystemMXBean to get precise CPU load
        final OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        scheduler.scheduleAtFixedRate(() ->
        {
            try
            {
                // Collect JVM Heap Metrics
                String formattedMetrics = getRetinaMetrics(osBean);

                logger.info("[Retina Metrics] {}", formattedMetrics);

            }
            catch (Exception e)
            {
                logger.error("Unexpected error in Retina metrics collection", e);
            }
        }, 0, logInterval, TimeUnit.SECONDS);
    }

    @FunctionalInterface
    interface BucketProcessor<T>
    {
        void process(int bucketId, List<T> subList, IndexOption option) throws Exception;
    }

    /**
     * A memory-efficient, read-only view that represents the transposed version of a list of objects.
     * This class implements the List interface but does not store the transposed data explicitly.
     * Instead, it computes the transposed data on-the-fly when accessed.
     */
    private static class TransposedIndexKeyView<T> extends AbstractList<List<IndexProto.IndexKey>>
    {
        private final List<T> originalData;
        private final Function<T, List<IndexProto.IndexKey>> indexExtractor;
        private final int columnCount;

        public TransposedIndexKeyView(List<T> originalData,
                                      Function<T, List<IndexProto.IndexKey>> indexExtractor)
        {
            this.originalData = originalData;
            this.indexExtractor = indexExtractor;
            if (originalData == null || originalData.isEmpty())
            {
                this.columnCount = 0;
            } else
            {
                this.columnCount = indexExtractor.apply(originalData.get(0)).size();
            }
        }

        @Override
        public List<IndexProto.IndexKey> get(int columnIndex)
        {
            if (columnIndex < 0 || columnIndex >= columnCount)
            {
                throw new IndexOutOfBoundsException("Column index out of bounds: " + columnIndex);
            }
            return new ColumnView(columnIndex);
        }

        @Override
        public int size()
        {
            return columnCount;
        }

        private class ColumnView extends AbstractList<IndexProto.IndexKey>
        {
            private final int columnIndex;

            public ColumnView(int columnIndex)
            {
                this.columnIndex = columnIndex;
            }

            @Override
            public IndexProto.IndexKey get(int rowIndex)
            {
                if (rowIndex < 0 || rowIndex >= originalData.size())
                {
                    throw new IndexOutOfBoundsException("Row index out of bounds: " + rowIndex);
                }
                return indexExtractor.apply(originalData.get(rowIndex)).get(columnIndex);
            }

            @Override
            public int size()
            {
                return originalData.size();
            }
        }
    }

    /**
     * In-memory lifecycle status of this Retina server. This struct is intentionally
     * kept inside RetinaServerImpl because it is never transmitted: outbound RPCs
     * project the relevant subset onto GetRetinaStatusResponse.
     */
    private static final class RetinaStatus
    {
        private final RetinaState state;
        private final String recoveryEpoch;
        private final Map<Integer, Long> vnodeReplayStarts;

        private RetinaStatus(Builder b)
        {
            this.state = b.state;
            this.recoveryEpoch = b.recoveryEpoch;
            this.vnodeReplayStarts = Collections.unmodifiableMap(new LinkedHashMap<>(b.vnodeReplayStarts));
        }

        static Builder newBuilder()
        {
            return new Builder();
        }

        Builder toBuilder()
        {
            Builder b = new Builder();
            b.state = this.state;
            b.recoveryEpoch = this.recoveryEpoch;
            b.vnodeReplayStarts = new LinkedHashMap<>(this.vnodeReplayStarts);
            return b;
        }

        String getRecoveryEpoch() { return recoveryEpoch; }
        Map<Integer, Long> getVnodeReplayStartsMap() { return vnodeReplayStarts; }
        RetinaState getState() { return state; }

        static final class Builder
        {
            private RetinaState state = RetinaState.UNKNOWN;
            private String recoveryEpoch = "";
            private Map<Integer, Long> vnodeReplayStarts = new LinkedHashMap<>();

            Builder setState(RetinaState v) { this.state = v; return this; }
            Builder setRecoveryEpoch(String v) { this.recoveryEpoch = v; return this; }
            Builder putAllVnodeReplayStarts(Map<Integer, Long> m)
            {
                this.vnodeReplayStarts.putAll(m);
                return this;
            }

            RetinaStatus build() { return new RetinaStatus(this); }
        }
    }
}
