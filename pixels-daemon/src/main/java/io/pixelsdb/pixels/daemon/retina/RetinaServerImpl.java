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
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.ResolvedPrimary;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.index.service.IndexServiceProvider;
import io.pixelsdb.pixels.common.index.service.LocalIndexService;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.IndexUtils;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RGVisibility;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.retina.RetinaResourceManager;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
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
            initializeRetinaResources();
            this.retinaResourceManager.startBackgroundGc();
            startRetinaMetricsLogThread();
            logger.info("Retina service is ready");
        }
        catch (Exception e)
        {
            logger.error("Error while initializing RetinaServerImpl", e);
            throw new IllegalStateException("Failed to initialize RetinaServerImpl", e);
        }
    }

    private void initializeRetinaResources() throws Exception
    {
        logger.info("Pre-loading checkpoints...");
        this.retinaResourceManager.recoverCheckpoints();

        List<Schema> schemas = this.metadataService.getSchemas();
        for (Schema schema : schemas)
        {
            List<Table> tables = this.metadataService.getTables(schema.getName());
            for (Table table : tables)
            {
                List<Layout> layouts = this.metadataService.getLayouts(schema.getName(), table.getName());
                List<String> files = new LinkedList<>();
                for (Layout layout : layouts)
                {
                    if (layout.isReadable())
                    {
                        /*
                         * Issue #946: always add visibility to all files
                         */
                        // add visibility for ordered files
                        List<Path> orderedPaths = layout.getOrderedPaths();
                        validateOrderedOrCompactPaths(orderedPaths);
                        List<File> orderedFiles = this.metadataService.getRegularFiles(orderedPaths.get(0).getId());
                        files.addAll(orderedFiles.stream()
                                .map(file -> orderedPaths.get(0).getUri() + "/" + file.getName())
                                .collect(Collectors.toList()));

                        // add visibility for compact files
                        List<Path> compactPaths = layout.getCompactPaths();
                        validateOrderedOrCompactPaths(compactPaths);
                        List<File> compactFiles = this.metadataService.getRegularFiles(compactPaths.get(0).getId());
                        files.addAll(compactFiles.stream()
                                .map(file -> compactPaths.get(0).getUri() + "/" + file.getName())
                                .collect(Collectors.toList()));
                    }
                }

                int threadNum = Integer.parseInt
                        (ConfigFactory.Instance().getProperty("retina.service.init.threads"));
                ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
                AtomicBoolean success = new AtomicBoolean(true);
                AtomicReference<Exception> e = new AtomicReference<>();
                try
                {
                    for (String filePath : files)
                    {
                        executorService.submit(() ->
                        {
                            try
                            {
                                this.retinaResourceManager.addVisibility(filePath);
                            }
                            catch (Exception ex)
                            {
                                success.set(false);
                                e.set(ex);
                            }
                        });
                    }
                }
                finally
                {
                    executorService.shutdown();
                }

                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

                if (!success.get())
                {
                    throw new RetinaException("Can't add visibility", e.get());
                }

                this.retinaResourceManager.addWriteBuffer(schema.getName(), table.getName());
            }
        }
    }

    /**
     * Check if the order or compact paths from pixels metadata is valid.
     *
     * @param paths the order or compact paths from pixels metadata.
     */
    public static void validateOrderedOrCompactPaths(List<Path> paths) throws RetinaException
    {
        requireNonNull(paths, "paths is null");
        checkArgument(!paths.isEmpty(), "paths must contain at least one valid directory");
        try
        {
            Storage.Scheme firstScheme = Storage.Scheme.fromPath(paths.get(0).getUri());
            assert firstScheme != null;
            for (int i = 1; i < paths.size(); ++i)
            {
                Storage.Scheme scheme = Storage.Scheme.fromPath(paths.get(i).getUri());
                checkArgument(firstScheme.equals(scheme),
                        "all the directories in the paths must have the same storage scheme");
            }
        }
        catch (Throwable e)
        {
            throw new RetinaException("Failed to parse storage scheme from paths", e);
        }
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
            headerBuilder.setErrorCode(1).setErrorMsg("Retina: " + e.getMessage());
            responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
        }
        catch (IndexException e)
        {
            logger.error("updateRecord failed for schema={} (index)", request.getSchemaName(), e);
            headerBuilder.setErrorCode(2).setErrorMsg("Index: " + e.getMessage());
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
                    headerBuilder.setErrorCode(1).setErrorMsg("Retina: " + e.getMessage());
                    responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                            .setHeader(headerBuilder.build())
                            .build());
                    logger.error("Error processing streaming update (retina)", e);
                }
                catch (IndexException e)
                {
                    headerBuilder.setErrorCode(2).setErrorMsg("Index: " + e.getMessage());
                    responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                            .setHeader(headerBuilder.build())
                            .build());
                    logger.error("Error processing streaming update (index)", e);
                }
                catch (Exception e)
                {
                    headerBuilder.setErrorCode(3).setErrorMsg("Internal error: " + e.getMessage());
                    responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                            .setHeader(headerBuilder.build())
                            .build());
                    logger.error("Unexpected error processing streaming update", e);
                }
                catch (Throwable t)
                {
                    headerBuilder.setErrorCode(4).setErrorMsg("Fatal error: " + t.getMessage());
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
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
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

            String checkpointPath = this.retinaResourceManager.getCheckpointPath(timestamp);
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
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
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
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
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
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
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
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
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
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
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
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
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
}
