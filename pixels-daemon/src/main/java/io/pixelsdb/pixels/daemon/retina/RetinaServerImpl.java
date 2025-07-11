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
    private final Map<String, RGVisibility> rgVisibilityMap;
    private final Map<String, PixelsWriterBuffer> pixelsWriterBufferMap;

    /**
     * Initialize the visibility management for all the records.
     */
    public RetinaServerImpl()
    {
        this.metadataService = MetadataService.Instance();
        this.rgVisibilityMap = new ConcurrentHashMap<>();
        this.pixelsWriterBufferMap = new ConcurrentHashMap<>();
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
                                List<Path> orderedPaths = layout.getOrderedPaths();
                                validateOrderedOrCompactPaths(orderedPaths);
                                List<File> orderedFiles = this.metadataService.getFiles(orderedPaths.get(0).getId());
                                files.addAll(orderedFiles.stream()
                                        .map(file -> orderedPaths.get(0).getUri() + "/" + file.getName())
                                        .collect(Collectors.toList()));
                            }
                            if (compactEnabled)
                            {
                                List<Path> compactPaths = layout.getCompactPaths();
                                validateOrderedOrCompactPaths(compactPaths);
                                List<File> compactFiles = this.metadataService.getFiles(compactPaths.get(0).getId());
                                files.addAll(compactFiles.stream()
                                        .map(file -> compactPaths.get(0).getUri() + "/" + file.getName())
                                        .collect(Collectors.toList()));
                            }
                        }
                    }
                    for (String filePath : files)
                    {
                        addVisibility(filePath);
                    }

                    addWriterBuffer(schema.getName(), table.getName());
                }
            }
        } catch (Exception e)
        {
            logger.error("Error while initializing RetinaServerImpl", e);
        }
    }

    public void deleteRecord(long fileId, int rgId, int rgRowId, long timestamp) throws RetinaException
    {
        try
        {
            RGVisibility rgVisibility = checkRGVisibility(fileId, rgId);
            rgVisibility.deleteRecord(rgRowId, timestamp);
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
        } catch (Exception e)
        {
            throw new RetinaException("Error while garbage collecting", e);
        }
    }

    public void addWriterBuffer(String schemaName, String tableName) throws RetinaException
    {
        try
        {
            // get ordered and compact dir path
            Layout latestLayout = this.metadataService.getLatestLayout(schemaName, tableName);
            List<Path> orderedPaths = latestLayout.getOrderedPaths();
            validateOrderedOrCompactPaths(orderedPaths);
            List<Path> compactPaths = latestLayout.getCompactPaths();
            validateOrderedOrCompactPaths(compactPaths);

            // get schema
            List<Column> columns = this.metadataService.getColumns(schemaName, tableName, false);
            List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
            List<String> columnTypes = columns.stream().map(Column::getType).collect(Collectors.toList());
            TypeDescription schema = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);

            PixelsWriterBuffer pixelsWriterBuffer = new PixelsWriterBuffer(schema, schemaName, tableName,
                    orderedPaths.get(0), compactPaths.get(0));
            String writerBufferKey = schemaName + "_" + tableName;
            pixelsWriterBufferMap.put(writerBufferKey, pixelsWriterBuffer);
        } catch (Exception e)
        {
            throw new RetinaException("Failed to add writer buffer for " + schemaName + "." + tableName, e);
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
            addWriterBuffer(request.getSchemaName(), request.getTableName());

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
    public void insertRecord(RetinaProto.InsertRecordRequest request,
                             StreamObserver<RetinaProto.InsertRecordResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try
        {
            PixelsWriterBuffer writerBuffer = checkPixelsWriterBuffer(request.getSchemaName(), request.getTableName());
            List<ByteString> colValuesList = request.getColValuesList();
            byte[][] colValuesByteArray = new byte[colValuesList.size()][];
            for (int i = 0; i < colValuesList.size(); ++i)
            {
                colValuesByteArray[i] = colValuesList.get(i).toByteArray();
            }
            writerBuffer.addRow(colValuesByteArray, request.getTimestamp());

            RetinaProto.InsertRecordResponse response = RetinaProto.InsertRecordResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RetinaException e)
        {
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.InsertRecordResponse.newBuilder()
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

            PixelsWriterBuffer writerBuffer = checkPixelsWriterBuffer(request.getSchemaName(), request.getTableName());
            SuperVersion currentVersion = writerBuffer.getCurrentVersion();
            if (!currentVersion.getMemTable().getRowBatch().isEmpty()) {
                ByteString data = ByteString.copyFrom(currentVersion.getMemTable().getRowBatch().serialize());
                responseBuilder.setData(data);
            } else {
                responseBuilder.setData(ByteString.EMPTY);
            }
            for (MemTable immutableMemtable : currentVersion.getImmutableMemTables())
            {
                responseBuilder.addIds(immutableMemtable.getId());
            }
            for (ObjectEntry objectEntry : currentVersion.getObjectEntries())
            {
                responseBuilder.addIds(objectEntry.getId());
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

    @Override
    public void deleteRecord(RetinaProto.DeleteRecordRequest request,
                             StreamObserver<RetinaProto.DeleteRecordResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try
        {
            long fileId = request.getFileId();
            int rgId = request.getRgId();
            int rgRowId = request.getRgRowId();
            long timestamp = request.getTimestamp();
            deleteRecord(fileId, rgId, rgRowId, timestamp);

            RetinaProto.DeleteRecordResponse response = RetinaProto.DeleteRecordResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RetinaException e)
        {
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.DeleteRecordResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void deleteRecords(RetinaProto.DeleteRecordsRequest request,
                              StreamObserver<RetinaProto.DeleteRecordsResponse> responseObserver)
    {
        RetinaProto.ResponseHeader.Builder headerBuilder = RetinaProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        try
        {
            long timestamp = request.getTimestamp();
            for (IndexProto.RowLocation row : request.getRowsList())
            {
                long fileId = row.getFileId();
                int rgId = row.getRgId();
                int rgRowId = row.getRgRowId();
                deleteRecord(fileId, rgId, rgRowId, timestamp);
            }

            RetinaProto.DeleteRecordsResponse response = RetinaProto.DeleteRecordsResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RetinaException e)
        {
            headerBuilder.setErrorCode(1).setErrorMsg(e.getMessage());
            responseObserver.onNext(RetinaProto.DeleteRecordsResponse.newBuilder()
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

    /**
     * Check if the retina exists for the given filePath and rgId.
     *
     * @param fileId the file id.
     * @param rgId the row group id.
     * @throws RetinaException if the retina does not exist.
     */
    private RGVisibility checkRGVisibility(long fileId, int rgId) throws RetinaException
    {
        try
        {
            String retinaKey = fileId + "_" + rgId;
            RGVisibility rgVisibility = this.rgVisibilityMap.get(retinaKey);
            if (rgVisibility == null)
            {
                throw new RetinaException("Retina not found for fileId: " + fileId + " and rgId: " + rgId);
            }
            return rgVisibility;
        } catch (Exception e)
        {
            throw new RetinaException("Error while checking retina", e);
        }
    }

    /**
     * Check if the writer buffer exists for the given schema and table
     */
    private PixelsWriterBuffer checkPixelsWriterBuffer(String schema, String table) throws RetinaException
    {
        try
        {
            String writerBufferKey = schema + "_" + table;
            PixelsWriterBuffer writerBuffer = this.pixelsWriterBufferMap.get(writerBufferKey);
            if (writerBuffer == null)
            {
                throw new RetinaException("Writer buffer not found for schema: " + schema + " and table: " + table);
            }
            return writerBuffer;
        } catch (Exception e)
        {
            throw new RetinaException("Error while checking writer buffer", e);
        }
    }
}