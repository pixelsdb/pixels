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

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.IndexService;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.retina.RetinaResourceManager;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
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
    private final ConcurrentHashMap<String, ReentrantLock> tableLocks = new ConcurrentHashMap<>();
    private final AtomicInteger optCount = new AtomicInteger(0);
    /**
     * Initialize the visibility management for all the records.
     */
    public RetinaServerImpl()
    {
        this.metadataService = MetadataService.Instance();
        this.indexService = IndexService.Instance();
        this.retinaResourceManager = RetinaResourceManager.Instance();
        try
        {
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
                            /**
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
                    for (String filePath : files)
                    {
                        this.retinaResourceManager.addVisibility(filePath);
                    }

                    this.retinaResourceManager.addWriterBuffer(schema.getName(), table.getName());
                }
            }
        } catch (Exception e)
        {
            logger.error("Error while initializing RetinaServerImpl", e);
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
                } catch (RetinaException | IndexException e)
                {
                    headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
                    responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                            .setHeader(headerBuilder.build())
                            .build());
                    logger.error("error processing streaming update", e);
                } catch (Exception e)
                {
                    headerBuilder.setErrorCode(2).setErrorMsg("Internal server error: " + e.getMessage());
                    responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                            .setHeader(headerBuilder.build())
                            .build());
                    logger.error("unexpected error processing streaming update", e);
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
        long timestamp = request.getTimestamp();
        int processUpdateId = optCount.getAndIncrement();
        logger.info("Receive Process Request {}", processUpdateId);
        if (!tableUpdateDataList.isEmpty())
        {
            for (RetinaProto.TableUpdateData tableUpdateData : tableUpdateDataList)
            {
                String tableName = tableUpdateData.getTableName();
                ReentrantLock lock = tableLocks.computeIfAbsent(tableName, k -> new ReentrantLock());
                lock.lock();
                try
                {
                    boolean init = true;
                    List<RetinaProto.DeleteData> deleteDataList = tableUpdateData.getDeleteDataList();
                    long primaryIndexId = tableUpdateData.getPrimaryIndexId();
                    if (!deleteDataList.isEmpty())
                    {
                        int indexNum = deleteDataList.get(0).getIndexKeysList().size();
                        List<List<IndexProto.IndexKey>> indexKeysList = new ArrayList<>(indexNum);
                        for (RetinaProto.DeleteData deleteData : deleteDataList)
                        {
                            List<IndexProto.IndexKey> deleteDataIndexKeysList = deleteData.getIndexKeysList();
                            if (deleteDataIndexKeysList.isEmpty())
                            {
                                throw new RetinaException("Delete index key list is empty");
                            }
                            extractIndexKeys(primaryIndexId, indexKeysList, deleteDataIndexKeysList, init);
                        }

                        List<IndexProto.IndexKey> primaryIndexKeys = indexKeysList.get(0);
                        long tableId = primaryIndexKeys.get(0).getTableId();
                        List<IndexProto.RowLocation> rowLocations = indexService.deletePrimaryIndexEntries
                                (tableId, primaryIndexId, primaryIndexKeys);
                        for (IndexProto.RowLocation rowLocation : rowLocations)
                        {
                            this.retinaResourceManager.deleteRecord(rowLocation, timestamp);
                        }

                        for (int i = 1; i < indexNum; i++)
                        {
                            List<IndexProto.IndexKey> indexKeys = indexKeysList.get(i);
                            indexService.deleteSecondaryIndexEntries
                                    (indexKeys.get(0).getTableId(), indexKeys.get(0).getIndexId(), indexKeys);
                        }
                    }

                    List<RetinaProto.InsertData> insertDataList = tableUpdateData.getInsertDataList();
                    if (!insertDataList.isEmpty())
                    {
                        int indexNum = insertDataList.get(0).getIndexKeysList().size();
                        List<List<IndexProto.IndexKey>> indexKeysList = new ArrayList<>(indexNum);
                        List<IndexProto.PrimaryIndexEntry> primaryIndexEntries = new ArrayList<>(insertDataList.size());
                        List<Long> rowIdList = new ArrayList<>(insertDataList.size());
                        for (RetinaProto.InsertData insertData : insertDataList)
                        {
                            List<IndexProto.IndexKey> insertDataIndexKeysList = insertData.getIndexKeysList();
                            if (insertDataIndexKeysList.isEmpty())
                            {
                                throw new RetinaException("Insert index key list is empty");
                            }
                            extractIndexKeys(primaryIndexId, indexKeysList, insertDataIndexKeysList, init);

                            List<ByteString> colValuesList = insertData.getColValuesList();
                            byte[][] colValuesByteArray = new byte[colValuesList.size()][];
                            for (int i = 0; i < colValuesList.size(); ++i)
                            {
                                colValuesByteArray[i] = colValuesList.get(i).toByteArray();
                            }
                            IndexProto.PrimaryIndexEntry.Builder primaryIndexEntryBuilder =
                                    this.retinaResourceManager.insertRecord(schemaName, tableName,
                                            colValuesByteArray, timestamp);
                            primaryIndexEntryBuilder.setIndexKey(insertData.getIndexKeys(0));
                            rowIdList.add(primaryIndexEntryBuilder.getRowId());
                            primaryIndexEntries.add(primaryIndexEntryBuilder.build());
                        }
                        long tableId = primaryIndexEntries.get(0).getIndexKey().getTableId();
                        indexService.putPrimaryIndexEntries(tableId, primaryIndexId, primaryIndexEntries);
                        for (int i = 1; i < indexNum; i++)
                        {
                            List<IndexProto.IndexKey> indexKeys = indexKeysList.get(i);
                            long indexId = indexKeys.get(0).getIndexId();
                            List<IndexProto.SecondaryIndexEntry> secondaryIndexEntries =
                                    IntStream.range(0, indexKeys.size())
                                            .mapToObj(j -> IndexProto.SecondaryIndexEntry.newBuilder()
                                                    .setRowId(rowIdList.get(j))
                                                    .setIndexKey(indexKeys.get(j))
                                                    .build())
                                            .collect(Collectors.toList());
                            indexService.putSecondaryIndexEntries
                                    (indexKeys.get(0).getTableId(), indexId, secondaryIndexEntries);
                        }
                    }

                } finally
                {
                    logger.info("Complete Process Request {}", processUpdateId);
                    lock.unlock();
                }

            }
        }
    }

    private void extractIndexKeys(long primaryIndexId, List<List<IndexProto.IndexKey>> indexKeysList,
                                  List<IndexProto.IndexKey> allIndexKeysList, boolean init) throws RetinaException
    {
        IndexProto.IndexKey primaryIndexKey = allIndexKeysList.get(0);
        if (primaryIndexKey.getIndexId() != primaryIndexId)
        {
            throw new RetinaException("Primary index id mismatch");
        }

        for (int i = 0; i < allIndexKeysList.size(); ++i)
        {
            IndexProto.IndexKey currIndexKey = allIndexKeysList.get(i);
            if (init)
            {
                indexKeysList.add(new ArrayList<>());
            } else
            {
                // check if indexId or tableId is mismatch
                IndexProto.IndexKey baseIndexKey = indexKeysList.get(i).get(0);
                if (baseIndexKey.getIndexId() != currIndexKey.getIndexId())
                {
                    throw new RetinaException("Index id mismatch");
                }
                if (baseIndexKey.getTableId() != currIndexKey.getTableId())
                {
                    throw new RetinaException("Table id mismatch");
                }
            }
            indexKeysList.get(i).add(currIndexKey);
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

            RetinaProto.QueryVisibilityResponse.Builder responseBuilder = RetinaProto.QueryVisibilityResponse
                    .newBuilder()
                    .setHeader(headerBuilder.build());

            for (int rgId : rgIds)
            {
                long[] visibilityBitmap = this.retinaResourceManager.queryVisibility(fileId, rgId, timestamp);
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
    public void addWriterBuffer(RetinaProto.AddWriterBufferRequest request,
                                StreamObserver<RetinaProto.AddWriterBufferResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        try
        {
            this.retinaResourceManager.addWriterBuffer(request.getSchemaName(), request.getTableName());

            RetinaProto.AddWriterBufferResponse response = RetinaProto.AddWriterBufferResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RetinaException e)
        {
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.AddWriterBufferResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getWriterBuffer(RetinaProto.GetWriterBufferRequest request,
                                StreamObserver<RetinaProto.GetWriterBufferResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try
        {
            RetinaProto.GetWriterBufferResponse.Builder response = this.retinaResourceManager.getWriterBuffer(
                    request.getSchemaName(), request.getTableName(), request.getTimestamp());
            response.setHeader(headerBuilder);

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (RetinaException e)
        {
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.GetWriterBufferResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
            responseObserver.onCompleted();
        }
    }

    /**
     * Check if the order or compact paths from pixels metadata is valid.
     *
     * @param paths the order or compact paths from pixels metadata.
     */
    public static void validateOrderedOrCompactPaths(List<Path> paths)
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
            throw new RuntimeException("failed to parse storage scheme from paths", e);
        }
    }

}