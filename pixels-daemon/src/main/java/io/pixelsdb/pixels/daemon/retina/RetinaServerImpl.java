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
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
    private final RetinaResourceManager retinaResourceManager;

    /**
     * Initialize the visibility management for all the records.
     */
    public RetinaServerImpl()
    {
        this.metadataService = MetadataService.Instance();
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
            String schemaName = request.getSchemaName();
            long timestamp = request.getTimestamp();

            // insert record
            List<RetinaProto.InsertData> insertDataList = request.getInsertDataList();
            if (!insertDataList.isEmpty())
            {
                for (RetinaProto.InsertData insertData : insertDataList)
                {
                    List<ByteString> colValuesList = insertData.getColValuesList();
                    byte[][] colValuesByteArray = new byte[colValuesList.size()][];
                    for (int i = 0; i < colValuesList.size(); ++i)
                    {
                        colValuesByteArray[i] = colValuesList.get(i).toByteArray();
                    }
                    // TODO: insert index
                    this.retinaResourceManager.insertData(schemaName, insertData.getTableName(),
                            colValuesByteArray, timestamp);
                }
            }

            // delete record
            List<RetinaProto.DeleteData> deleteDataList = request.getDeleteDataList();
            if (!deleteDataList.isEmpty())
            {
                for (RetinaProto.DeleteData deleteData : deleteDataList)
                {
                    // TODO: delete index
                    IndexProto.RowLocation rowLocation = null; // returned by deleteIndex
                    this.retinaResourceManager.deleteRecord(rowLocation, timestamp);
                }
            }
        } catch (RetinaException e)
        {
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.UpdateRecordResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
            responseObserver.onCompleted();
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
    public void getSuperVersion(RetinaProto.GetSuperVersionRequest request,
                                StreamObserver<RetinaProto.GetSuperVersionResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try
        {
            RetinaProto.GetSuperVersionResponse.Builder responseBuilder = RetinaProto.GetSuperVersionResponse
                    .newBuilder()
                    .setHeader(headerBuilder.build());

            String schemaName = request.getSchemaName();
            String tableName = request.getTableName();
            SuperVersion currentVersion = this.retinaResourceManager.getSuperVersion(schemaName, tableName);
            if (!currentVersion.getMemTable().getRowBatch().isEmpty()) {
                ByteString data = ByteString.copyFrom(currentVersion.getMemTable().getRowBatch().serialize());
                responseBuilder.setData(data);
            } else {
                responseBuilder.setData(ByteString.EMPTY);
            }

            List<Long> ids = new ArrayList<>();
            for (MemTable immutableMemtable : currentVersion.getImmutableMemTables())
            {
                ids.add(immutableMemtable.getId());
            }
            for (ObjectEntry objectEntry : currentVersion.getObjectEntries())
            {
                ids.add(objectEntry.getId());
            }
            responseBuilder.addAllIds(ids);
            long[][] visibilityBitmaps = this.retinaResourceManager.getWriterBufferVisibility(
                    schemaName, tableName, ids, request.getTimestamp());
            for (long[] visibilityBitmap : visibilityBitmaps)
            {
                RetinaProto.VisibilityBitmap bitmap = RetinaProto.VisibilityBitmap.newBuilder()
                        .addAllBitmap(Arrays.stream(visibilityBitmap).boxed().collect(Collectors.toList()))
                        .build();
                responseBuilder.addBitmaps(bitmap);
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e)
        {
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.GetSuperVersionResponse.newBuilder()
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