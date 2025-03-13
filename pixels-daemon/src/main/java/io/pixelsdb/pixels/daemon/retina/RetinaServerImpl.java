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

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import io.pixelsdb.pixels.retina.RGVisibility;
import io.pixelsdb.pixels.retina.RetinaProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
    private final Map<String, RGVisibility> rgVisibilityMap;

    /**
     * Initialize the visibility management for all the records.
     */
    public RetinaServerImpl()
    {
        this.metadataService = MetadataService.Instance();
        this.rgVisibilityMap = new ConcurrentHashMap<>();
        try
        {
            boolean orderedEnabled = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("executor.ordered.layout.enabled"));
            boolean compactEnabled = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("executor.compact.layout.enabled"));
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
                            if (orderedEnabled)
                            {
                                String[] orderedPaths = layout.getOrderedPathUris();
                                validateOrderedOrCompactPaths(orderedPaths);
                                Storage storage = StorageFactory.Instance().getStorage(orderedPaths[0]);
                                files.addAll(storage.listPaths(orderedPaths));
                            }
                            if (compactEnabled)
                            {
                                String[] compactPaths = layout.getCompactPathUris();
                                validateOrderedOrCompactPaths(compactPaths);
                                Storage storage = StorageFactory.Instance().getStorage(compactPaths[0]);
                                files.addAll(storage.listPaths(compactPaths));
                            }
                        }
                    }
                    for (String filePath : files)
                    {
                        addVisibility(filePath);
                    }
                }
            }
        } catch (Exception e)
        {
            logger.error("Error while initializing RetinaServerImpl", e);
        }
    }

    public void deleteRecord(String filePath, int rgId, long rowId, long timestamp) throws RetinaException
    {
        try
        {
            RGVisibility rgVisibility = checkRGVisibility(filePath, rgId);
            rgVisibility.deleteRecord(rowId, timestamp);
        } catch (Exception e)
        {
            throw new RetinaException("Error while deleting record", e);
        }
    }

    public void addVisibility(String filePath) throws RetinaException
    {
        try
        {
            Storage storage = StorageFactory.Instance().getStorage(filePath);
            PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(storage, filePath);
            long fileLen = fsReader.getFileLength();
            fsReader.seek(fileLen - Long.BYTES);
            long fileTailOffset = fsReader.readLong(ByteOrder.BIG_ENDIAN);
            int fileTailLength = (int) (fileLen - fileTailOffset - Long.BYTES);
            fsReader.seek(fileTailOffset);
            ByteBuffer fileTailBuffer = fsReader.readFully(fileTailLength);
            PixelsProto.FileTail fileTail = PixelsProto.FileTail.parseFrom(fileTailBuffer);
            PixelsProto.Footer footer = fileTail.getFooter();
            long fileId = this.metadataService.getFileId(filePath);
            for (int rgId = 0; rgId < footer.getRowGroupInfosCount(); rgId++)
            {
                int recordNum = footer.getRowGroupInfos(rgId).getNumberOfRows();
                RGVisibility rgVisibility = new RGVisibility(recordNum);
                String rgKey = fileId + "_" + rgId;
                rgVisibilityMap.put(rgKey, rgVisibility);
            }
        } catch (Exception e)
        {
            throw new RetinaException("Error while adding visibility", e);
        }
    }

    public long[] queryVisibility(String filePath, int rgId, long timestamp) throws RetinaException
    {
        try
        {
            RGVisibility rgVisibility = checkRGVisibility(filePath, rgId);
            long[] visibilityBitmap = rgVisibility.getVisibilityBitmap(timestamp);
            if (visibilityBitmap == null)
            {
                throw new RetinaException("Visibility bitmap not found for filePath: " + filePath + " and rgId: " + rgId);
            }
            return visibilityBitmap;
        } catch (Exception e)
        {
            throw new RetinaException("Error while getting visibility bitmap", e);
        }
    }

    public void garbageCollect(String filePath, int rgId, long timestamp) throws RetinaException
    {
        try
        {
            RGVisibility rgVisibility = checkRGVisibility(filePath, rgId);
            rgVisibility.garbageCollect(timestamp);
        } catch (Exception e) {
            throw new RetinaException("Error while garbage collecting", e);
        }
    }
    
    @Override
    public void deleteRecord(RetinaProto.DeleteRecordRequest request,
                             StreamObserver<RetinaProto.DeleteRecordResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try 
        {
            String filePath = request.getFilePath();
            int rgId = request.getRgId();
            long rowId = request.getRowId();
            long timestamp = request.getTimestamp();
            deleteRecord(filePath, rgId, rowId, timestamp);

            RetinaProto.DeleteRecordResponse response = RetinaProto.DeleteRecordResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RetinaException e) {
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.DeleteRecordResponse.newBuilder()
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
            addVisibility(filePath);

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
            String filePath = request.getFilePath();
            int[] rgIds = request.getRgIdsList().stream().mapToInt(Integer::intValue).toArray();
            long timestamp = request.getTimestamp();

            RetinaProto.QueryVisibilityResponse.Builder responseBuilder = RetinaProto.QueryVisibilityResponse
                    .newBuilder()
                    .setHeader(headerBuilder.build());
            
            for (int rgId : rgIds)
            {
                long[] visibilityBitmap = queryVisibility(filePath, rgId, timestamp);
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
    public void garbageCollect(RetinaProto.GarbageCollectRequest request,
                              StreamObserver<RetinaProto.GarbageCollectResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try
        {
            String filePath = request.getFilePath();
            int[] rgIds = request.getRgIdsList().stream().mapToInt(Integer::intValue).toArray();
            long timestamp = request.getTimestamp();
            for (int rgId : rgIds)
            {
                garbageCollect(filePath, rgId, timestamp);
            }

            RetinaProto.GarbageCollectResponse response = RetinaProto.GarbageCollectResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RetinaException e) 
        {
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.GarbageCollectResponse.newBuilder()
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
    public static void validateOrderedOrCompactPaths(String[] paths)
    {
        requireNonNull(paths, "paths is null");
        checkArgument(paths.length > 0, "paths must contain at least one valid directory");
        try 
        {
            Storage.Scheme firstScheme = Storage.Scheme.fromPath(paths[0]);
            for (int i = 1; i < paths.length; ++i) 
            {
                Storage.Scheme scheme = Storage.Scheme.fromPath(paths[i]);
                checkArgument(firstScheme.equals(scheme),
                        "all the directories in the paths must have the same storage scheme");
            }
        } catch (Throwable e) {
            throw new RuntimeException("failed to parse storage scheme from paths", e);
        }
    }
    
    /**
     * Check if the retina exists for the given filePath and rgId.
     * 
     * @param filePath the file path.
     * @param rgId the row group id.
     * @throws RetinaException if the retina does not exist.
     */
    private RGVisibility checkRGVisibility(String filePath, int rgId) throws RetinaException
    {
        try
        {
            long fileId = this.metadataService.getFileId(filePath);
            String retinaKey = fileId + "_" + rgId;
            RGVisibility rgVisibility = this.rgVisibilityMap.get(retinaKey);
            if (rgVisibility == null)
            {
                throw new RetinaException("Retina not found for filePath: " + filePath + " and rgId: " + rgId);
            }
            return rgVisibility;
        } catch (Exception e)
        {
            throw new RetinaException("Error while checking retina", e);
        }
    }
}
