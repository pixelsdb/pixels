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
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.IndexService;
import io.pixelsdb.pixels.common.index.IndexServiceProvider;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.retina.RetinaResourceManager;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
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

    /**
     * Initialize the visibility management for all the records.
     */
    public RetinaServerImpl()
    {
        this.metadataService = MetadataService.Instance();
        this.indexService = IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.local);
        this.retinaResourceManager = RetinaResourceManager.Instance();
        try
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
                            List<File> orderedFiles = this.metadataService.getFiles(orderedPaths.get(0).getId());
                            files.addAll(orderedFiles.stream()
                                    .map(file -> orderedPaths.get(0).getUri() + "/" + file.getName())
                                    .collect(Collectors.toList()));

                            // add visibility for compact files
                            List<Path> compactPaths = layout.getCompactPaths();
                            validateOrderedOrCompactPaths(compactPaths);
                            List<File> compactFiles = this.metadataService.getFiles(compactPaths.get(0).getId());
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
                                } catch (Exception ex)
                                {
                                    success.set(false);
                                    e.set(ex);
                                }
                            });
                        }
                    } finally
                    {
                        executorService.shutdown();
                    }

                    if(success.get())
                    {
                        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                    }

                    if(!success.get())
                    {
                        throw new RetinaException("Can't add visibility", e.get());
                    }

                    this.retinaResourceManager.addWriteBuffer(schema.getName(), table.getName());
                }
            }
            this.retinaResourceManager.finishRecovery();
            logger.info("Retina service is ready");
        } catch (Exception e)
        {
            logger.error("Error while initializing RetinaServerImpl", e);
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
        } catch (Throwable e)
        {
            throw new RetinaException("Failed to parse storage scheme from paths", e);
        }
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
        } catch (RetinaException | IndexException e)
        {
            logger.error("updateRecord failed for schema={}", request.getSchemaName(), e);
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
        } finally
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
                } catch (RetinaException e)
                {
                    headerBuilder.setErrorCode(1).setErrorMsg("Retina: " + e.getMessage());
                    responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                            .setHeader(headerBuilder.build())
                            .build());
                    logger.error("Error processing streaming update (retina)", e);
                } catch (IndexException e)
                {
                    headerBuilder.setErrorCode(2).setErrorMsg("Index: " + e.getMessage());
                    responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                            .setHeader(headerBuilder.build())
                            .build());
                    logger.error("Error processing streaming update (index)", e);
                } catch (Exception e)
                {
                    headerBuilder.setErrorCode(3).setErrorMsg("Internal error: " + e.getMessage());
                    responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                            .setHeader(headerBuilder.build())
                            .build());
                    logger.error("Unexpected error processing streaming update", e);
                } catch (Throwable t)
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
        if (!tableUpdateDataList.isEmpty())
        {
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
                    // 1a. Validate the delete data
                    int indexNum = deleteDataList.get(0).getIndexKeysList().size();
                    if (indexNum == 0)
                    {
                        throw new RetinaException("Delete index key list is empty");
                    }

                    boolean allRecordsValid = deleteDataList.stream().allMatch(deleteData ->
                            deleteData.getIndexKeysCount() == indexNum &&
                                    deleteData.getIndexKeys(0).getIndexId() == primaryIndexId);
                    if (!allRecordsValid)
                    {
                        throw new RetinaException("Primary index id mismatch or inconsistent index key list size");
                    }

                    // 1b. Transpose the index keys from row set to column set
                    List<List<IndexProto.IndexKey>> indexKeysList = transposeIndexKeys(
                            deleteDataList, RetinaProto.DeleteData::getIndexKeysList);

                    // 1c. Delete the primary index entries and get the row locations
                    List<IndexProto.IndexKey> primaryIndexKeys = indexKeysList.get(0);
                    long tableId = primaryIndexKeys.get(0).getTableId();
                    List<IndexProto.RowLocation> rowLocations = indexService.deletePrimaryIndexEntries
                            (tableId, primaryIndexId, primaryIndexKeys);

                    // 1d. Delete the records
                    for (IndexProto.RowLocation rowLocation : rowLocations)
                    {
                        this.retinaResourceManager.deleteRecord(rowLocation, timestamp);
                    }

                    // 1e. Delete the secondary index entries
                    for (int i = 1; i < indexNum; ++i)
                    {
                        List<IndexProto.IndexKey> indexKeys = indexKeysList.get(i);
                        indexService.deleteSecondaryIndexEntries(indexKeys.get(0).getTableId(),
                                indexKeys.get(0).getIndexId(), indexKeys);
                    }
                }

                // =================================================================
                // 2. Process Insert Data
                // =================================================================
                List<RetinaProto.InsertData> insertDataList = tableUpdateData.getInsertDataList();
                int virtualNodeId = request.getVirtualNodeId();
                if (!insertDataList.isEmpty())
                {
                    // 2a. Validate the insert data
                    int indexNum = insertDataList.get(0).getIndexKeysList().size();
                    if (indexNum == 0)
                    {
                        throw new RetinaException("Insert index key list is empty");
                    }

                    boolean allRecordValid = insertDataList.stream().allMatch(insertData ->
                            insertData.getIndexKeysCount() == indexNum &&
                                    insertData.getIndexKeys(0).getIndexId() == primaryIndexId);
                    if (!allRecordValid)
                    {
                        throw new RetinaException("Primary index id mismatch or inconsistent index key list size");
                    }

                    // 2b. Transpose the index keys from row set to column set
                    List<List<IndexProto.IndexKey>> indexKeysList = transposeIndexKeys(
                            insertDataList, RetinaProto.InsertData::getIndexKeysList);

                    // 2c. Insert the records and get the primary index entries
                    List<IndexProto.PrimaryIndexEntry> primaryIndexEntries = new ArrayList<>(insertDataList.size());
                    List<Long> rowIdList = new ArrayList<>(insertDataList.size());

                    for (RetinaProto.InsertData insertData : insertDataList)
                    {
                        byte[][] colValuesByteArray = insertData.getColValuesList().stream()
                                .map(ByteString::toByteArray)
                                .toArray(byte[][]::new);

                        IndexProto.PrimaryIndexEntry.Builder builder =
                                this.retinaResourceManager.insertRecord(schemaName, tableName,
                                        colValuesByteArray, timestamp, virtualNodeId);
                        builder.setIndexKey(insertData.getIndexKeys(0));
                        IndexProto.PrimaryIndexEntry entry = builder.build();
                        primaryIndexEntries.add(entry);
                        rowIdList.add(entry.getRowId());
                    }

                    // 2d. Put the primary index entries
                    long tableId = primaryIndexEntries.get(0).getIndexKey().getTableId();
                    indexService.putPrimaryIndexEntries(tableId, primaryIndexId, primaryIndexEntries);

                    // 2e. Put the secondary index entries
                    for (int i = 1; i < indexNum; ++i)
                    {
                        List<IndexProto.IndexKey> indexKeys = indexKeysList.get(i);
                        List<IndexProto.SecondaryIndexEntry> secondaryIndexEntries =
                                IntStream.range(0, indexKeys.size())
                                        .mapToObj(j -> IndexProto.SecondaryIndexEntry.newBuilder()
                                                .setRowId(rowIdList.get(j))
                                                .setIndexKey(indexKeys.get(j))
                                                .build())
                                        .collect(Collectors.toList());
                        indexService.putSecondaryIndexEntries(indexKeys.get(0).getTableId(),
                                indexKeys.get(0).getIndexId(), secondaryIndexEntries);
                    }
                }

                // =================================================================
                // 3. Process Update Data
                // =================================================================
                List<RetinaProto.UpdateData> updateDataList = tableUpdateData.getUpdateDataList();
                if (!updateDataList.isEmpty())
                {
                    // 3a. Validate the update data
                    int indexNum = updateDataList.get(0).getIndexKeysList().size();
                    if (indexNum == 0)
                    {
                        throw new RetinaException("Update index key list is empty");
                    }

                    boolean allRecordsValid = updateDataList.stream().allMatch(updateData ->
                            updateData.getIndexKeysCount() == indexNum &&
                                    updateData.getIndexKeys(0).getIndexId() == primaryIndexId);
                    if (!allRecordsValid)
                    {
                        throw new RetinaException("Primary index id mismatch or inconsistent index key list size");
                    }

                    // 3b. Transpose the index keys from row set to column set
                    List<List<IndexProto.IndexKey>> indexKeysList = transposeIndexKeys(
                            updateDataList, RetinaProto.UpdateData::getIndexKeysList);

                    // 3c. Insert the records and get the primary index entries
                    List<IndexProto.PrimaryIndexEntry> primaryIndexEntries = new ArrayList<>(updateDataList.size());
                    List<Long> rowIdList = new ArrayList<>(updateDataList.size());

                    for (RetinaProto.UpdateData updateData : updateDataList)
                    {
                        byte[][] colValuesByteArray = updateData.getColValuesList().stream()
                                .map(ByteString::toByteArray)
                                .toArray(byte[][]::new);

                        IndexProto.PrimaryIndexEntry.Builder builder =
                                this.retinaResourceManager.insertRecord(schemaName, tableName,
                                        colValuesByteArray, timestamp, virtualNodeId);

                        builder.setIndexKey(updateData.getIndexKeys(0));
                        IndexProto.PrimaryIndexEntry entry = builder.build();
                        primaryIndexEntries.add(entry);
                        rowIdList.add(entry.getRowId());
                    }

                    // 3d. Update the primary index entries and get the previous row locations
                    long tableId = primaryIndexEntries.get(0).getIndexKey().getTableId();
                    String lockKey = "vnode_" + virtualNodeId + "_idx_" + primaryIndexId;
                    Lock lock = updateLocks.get(lockKey);


                    List<IndexProto.RowLocation> previousRowLocations = null;
                    lock.lock();
                    try
                    {
                        previousRowLocations = indexService.updatePrimaryIndexEntries
                                (tableId, primaryIndexId, primaryIndexEntries);
                    } finally
                    {
                        lock.unlock();
                    }

                    // 3e. Delete the previous records
                    for (IndexProto.RowLocation location : previousRowLocations)
                    {
                        this.retinaResourceManager.deleteRecord(location, timestamp);
                    }

                    // 3f. Update the secondary index entries
                    for (int i = 1; i < indexNum; ++i)
                    {
                        List<IndexProto.IndexKey> indexKeys = indexKeysList.get(i);
                        List<IndexProto.SecondaryIndexEntry> secondaryIndexEntries =
                                IntStream.range(0, indexKeys.size())
                                        .mapToObj(j -> IndexProto.SecondaryIndexEntry.newBuilder()
                                                .setRowId(rowIdList.get(j))
                                                .setIndexKey(indexKeys.get(j))
                                                .build())
                                        .collect(Collectors.toList());

                        indexService.updateSecondaryIndexEntries(indexKeys.get(0).getTableId(),
                                indexKeys.get(0).getIndexId(), secondaryIndexEntries);
                    }

                }
            }
        }
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
        } catch (RetinaException e)
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

            for (int rgId : rgIds)
            {
                long[] visibilityBitmap = this.retinaResourceManager.queryVisibility(fileId, rgId, timestamp, transId);
                RetinaProto.VisibilityBitmap bitmap = RetinaProto.VisibilityBitmap.newBuilder()
                        .addAllBitmap(Arrays.stream(visibilityBitmap).boxed().collect(Collectors.toList()))
                        .build();
                responseBuilder.addBitmaps(bitmap);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (RetinaException e)
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
        } catch (RetinaException e)
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
        } catch (RetinaException e)
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
        } catch (RetinaException e)
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
        } catch (RetinaException e)
        {
            logger.error("registerOffload failed for timestamp={}",
                    request.getTimestamp(), e);
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.RegisterOffloadResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build());
        } finally
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
        } catch (Exception e)
        {
            logger.error("unregisterOffload failed for timestamp={}",
                    request.getTimestamp(), e);
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.UnregisterOffloadResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build());
        } finally
        {
            responseObserver.onCompleted();
        }
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
