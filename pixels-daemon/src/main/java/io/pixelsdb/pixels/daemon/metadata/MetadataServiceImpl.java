/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.daemon.metadata;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ProtocolStringList;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.MetadataServiceGrpc;
import io.pixelsdb.pixels.daemon.metadata.dao.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static io.pixelsdb.pixels.common.error.ErrorCode.*;

/**
 * @author hank
 * @create 2019-04-16
 * @update 2023-06-10 add peer, path, and peerPath related methods.
 */
public class MetadataServiceImpl extends MetadataServiceGrpc.MetadataServiceImplBase
{
    private static final Logger log = LogManager.getLogger(MetadataServiceImpl.class);

    private final SchemaDao schemaDao = DaoFactory.Instance().getSchemaDao();
    private final TableDao tableDao = DaoFactory.Instance().getTableDao();
    private final ColumnDao columnDao = DaoFactory.Instance().getColumnDao();
    private final LayoutDao layoutDao = DaoFactory.Instance().getLayoutDao();
    private final ViewDao viewDao = DaoFactory.Instance().getViewDao();
    private final PathDao pathDao = DaoFactory.Instance().getPathDao();
    private final PeerDao peerDao = DaoFactory.Instance().getPeerDao();
    private final PeerPathDao peerPathDao = DaoFactory.Instance().getPeerPathDao();
    private final SchemaVersionDao schemaVersionDao = DaoFactory.Instance().getSchemaVersionDao();

    public MetadataServiceImpl () { }

    /**
     * Build the initial schema version proto object without range index.
     * @param tableId the id of the table that the schema version belongs to
     * @param columns the columns owned by the schema version
     * @param transTs the transaction timestamp (i.e., version) of the schema version
     * @return the initial schema version, the id of the schema version is not set
     */
    public static MetadataProto.SchemaVersion buildInitSchemaVersion(long tableId, List<MetadataProto.Column> columns, long transTs)
    {
        return MetadataProto.SchemaVersion.newBuilder()
                .addAllColumns(columns).setTransTs(transTs).setTableId(tableId).build();
    }

    /**
     * Build the initial data layout of the given table and schema version.
     * @param tableId the id of the given table
     * @param schemaVersionId the id of the given schema version
     * @param columns the columns owned by the data layout
     * @return the initial data layout, the id of the layout is not set
     */
    public static MetadataProto.Layout buildInitLayout(long tableId, long schemaVersionId, List<MetadataProto.Column> columns)
    {
        Ordered ordered = new Ordered();
        for (MetadataProto.Column column : columns)
        {
            ordered.addColumnOrder(column.getName());
        }
        int compactFactor = Integer.parseInt(ConfigFactory.Instance().getProperty("compact.factor"));
        OriginSplitPattern splitPattern = new OriginSplitPattern();
        Compact compact = new Compact();
        compact.setNumColumn(columns.size());
        compact.setNumRowGroupInFile(compactFactor);
        compact.setCacheBorder(0); // cache is disabled for initial layout
        for (int columnId = 0; columnId < columns.size(); ++ columnId)
        {
            for (int rowGroupId = 0; rowGroupId < compactFactor; ++ rowGroupId)
            {
                // build a fully compact layout with all the chunks of the same column stored together
                compact.addColumnChunkOrder(rowGroupId, columnId);
            }
            // build the only split pattern that contains all the columns
            splitPattern.addAccessedColumns(columnId);
        }
        splitPattern.setNumRowGroupInSplit(compactFactor);
        Splits splits = new Splits();
        splits.setNumRowGroupInFile(compactFactor);
        splits.addSplitPatterns(splitPattern);
        // build an empty projection
        Projections projections = new Projections();
        projections.setNumProjections(0);
        projections.setProjectionPatterns(ImmutableList.of());
        return MetadataProto.Layout.newBuilder()
                .setVersion(0) // the version of layout starts from 0
                .setCreateAt(System.currentTimeMillis())
                .setPermission(MetadataProto.Permission.READ_WRITE) // the initial layout should be writable
                .setOrdered(JSON.toJSONString(ordered))
                .setCompact(JSON.toJSONString(compact))
                .setSplits(JSON.toJSONString(splits))
                .setProjections(JSON.toJSONString(projections))
                .setSchemaVersionId(schemaVersionId)
                .setTableId(tableId).build(); // no need to set the ordered paths and compact paths
    }

    /**
     * Build the initial paths for the given layout, without attaching to any ranges.
     * @param layoutId the id of the given layout
     * @param layoutVersion the version of the given layout
     * @param basePathUris the URIs of the base paths
     * @param isCompact whether the paths are compact paths
     * @return the initial paths, the ids and range ids of the paths are not set
     */
    public static List<MetadataProto.Path> buildInitPaths(long layoutId, long layoutVersion,
                                                          ProtocolStringList basePathUris, boolean isCompact)
    {
        ImmutableList.Builder<MetadataProto.Path> pathsBuilder = ImmutableList.builderWithExpectedSize(basePathUris.size());
        for (String basePathUri : basePathUris)
        {
            if (!basePathUri.endsWith("/"))
            {
                basePathUri += "/";
            }
            if (isCompact)
            {
                pathsBuilder.add(MetadataProto.Path.newBuilder()
                        .setUri(basePathUri + "v-" + layoutVersion + "-compact")
                        .setIsCompact(true).setLayoutId(layoutId).build());
            }
            else
            {
                pathsBuilder.add(MetadataProto.Path.newBuilder()
                        .setUri(basePathUri + "v-" + layoutVersion + "-ordered")
                        .setIsCompact(false).setLayoutId(layoutId).build());
            }
        }
        return pathsBuilder.build();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getSchemas(MetadataProto.GetSchemasRequest request, StreamObserver<MetadataProto.GetSchemasResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        List<MetadataProto.Schema> schemas = this.schemaDao.getAll();
        MetadataProto.ResponseHeader header;
        MetadataProto.GetSchemasResponse response;
        /**
         * Issue #156:
         * schemas.isEmpty() is normal when there is no existing schema.
         */
        if (schemas == null)
        {
            header = headerBuilder.setErrorCode(METADATA_SCHEMA_NOT_FOUND)
                    .setErrorMsg("metadata server failed to get schemas").build();
            response = MetadataProto.GetSchemasResponse.newBuilder().setHeader(header).build();
        }
        else
        {
            header = headerBuilder.setErrorCode(0).setErrorMsg("").build();
            response = MetadataProto.GetSchemasResponse.newBuilder().setHeader(header)
                .addAllSchemas(schemas).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getTable(MetadataProto.GetTableRequest request, StreamObserver<MetadataProto.GetTableResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        MetadataProto.ResponseHeader header;
        MetadataProto.GetTableResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        MetadataProto.Table table;

        if(schema != null)
        {
            table = tableDao.getByNameAndSchema(request.getTableName(), schema);
            if (table == null)
            {
                header = headerBuilder.setErrorCode(METADATA_TABLE_NOT_FOUND)
                        .setErrorMsg("metadata server failed to get table").build();
                response = MetadataProto.GetTableResponse.newBuilder()
                        .setHeader(header).build();
            }
            else
            {
                header = headerBuilder.setErrorCode(0).setErrorMsg("").build();
                response = MetadataProto.GetTableResponse.newBuilder()
                        .setHeader(header)
                        .setTable(table).build();
            }
        }
        else
        {
            header = headerBuilder.setErrorCode(METADATA_SCHEMA_NOT_FOUND).setErrorMsg("schema '" +
                    request.getSchemaName() + "' not found").build();
            response = MetadataProto.GetTableResponse.newBuilder().setHeader(header).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateRowCount(MetadataProto.UpdateRowCountRequest request, StreamObserver<MetadataProto.UpdateRowCountResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        MetadataProto.ResponseHeader header;
        MetadataProto.UpdateRowCountResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        MetadataProto.Table table;

        if(schema != null)
        {
            table = tableDao.getByNameAndSchema(request.getTableName(), schema);
            if (table == null)
            {
                header = headerBuilder.setErrorCode(METADATA_TABLE_NOT_FOUND)
                        .setErrorMsg("table not found").build();
            }
            else
            {
                table = table.toBuilder().setRowCount(request.getRowCount()).build();
                if (tableDao.update(table))
                {
                    header = headerBuilder.setErrorCode(0).setErrorMsg("").build();
                }
                else
                {
                    header = headerBuilder.setErrorCode(METADATA_UPDATE_TABLE_FAILED)
                            .setErrorMsg("metadata service failed to update table row count").build();
                }
            }
        }
        else
        {
            header = headerBuilder.setErrorCode(METADATA_SCHEMA_NOT_FOUND).setErrorMsg("schema '" +
                    request.getSchemaName() + "' not found").build();
        }
        response = MetadataProto.UpdateRowCountResponse.newBuilder().setHeader(header).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getTables(MetadataProto.GetTablesRequest request, StreamObserver<MetadataProto.GetTablesResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        MetadataProto.ResponseHeader header;
        MetadataProto.GetTablesResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        List<MetadataProto.Table> tables;

        if(schema != null)
        {
            tables = tableDao.getBySchema(schema);
            /**
             * Issue #85:
             * tables.isEmpty() is normal for empty schema.
             */
            if (tables == null)
            {
                header = headerBuilder.setErrorCode(METADATA_TABLE_NOT_FOUND)
                        .setErrorMsg("metadata server failed to get tables").build();
                response = MetadataProto.GetTablesResponse.newBuilder()
                        .setHeader(header).build();
            }
            else
            {
                header = headerBuilder.setErrorCode(0).setErrorMsg("").build();
                response = MetadataProto.GetTablesResponse.newBuilder()
                        .setHeader(header)
                        .addAllTables(tables).build();
            }
        }
        else
        {
            header = headerBuilder.setErrorCode(METADATA_SCHEMA_NOT_FOUND).setErrorMsg("schema '" +
                    request.getSchemaName() + "' not found").build();
            response = MetadataProto.GetTablesResponse.newBuilder().setHeader(header).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getLayouts(MetadataProto.GetLayoutsRequest request, StreamObserver<MetadataProto.GetLayoutsResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        MetadataProto.GetLayoutsResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        List<MetadataProto.Layout> layouts = null;
        if(schema != null)
        {
            MetadataProto.Table table = tableDao.getByNameAndSchema(request.getTableName(), schema);
            if (table != null)
            {
                layouts = layoutDao.getByTable(table, -1,
                        MetadataProto.GetLayoutRequest.PermissionRange.READABLE); // version < 0 means get all versions
                if (layouts == null || layouts.isEmpty())
                {
                    headerBuilder.setErrorCode(METADATA_LAYOUT_NOT_FOUND).setErrorMsg("no layout for table '" +
                            request.getSchemaName() + "." + request.getTableName() + "'");
                }
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_TABLE_NOT_FOUND).setErrorMsg("table '" +
                        request.getSchemaName() + "." + request.getTableName() + "' not found");
            }
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_SCHEMA_NOT_FOUND).setErrorMsg("schema '" + request.getSchemaName() + "' not found");
        }
        if(layouts != null && layouts.isEmpty() == false)
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.GetLayoutsResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .addAllLayouts(layouts).build();
        }
        else
        {
            response = MetadataProto.GetLayoutsResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getLayout(MetadataProto.GetLayoutRequest request, StreamObserver<MetadataProto.GetLayoutResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        MetadataProto.GetLayoutResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        MetadataProto.Layout layout = null;
        if(schema != null)
        {
            MetadataProto.Table table = tableDao.getByNameAndSchema(request.getTableName(), schema);
            if (table != null)
            {
                if (request.getLayoutVersion() < 0)
                {
                    layout = layoutDao.getLatestByTable(table, request.getPermissionRange());
                    if (layout == null)
                    {
                        headerBuilder.setErrorCode(METADATA_LAYOUT_NOT_FOUND).setErrorMsg("layout of table '" +
                                request.getSchemaName() + "." + request.getTableName() + "' with permission '" +
                                request.getPermissionRange().name() + "' not found");
                    }
                }
                else
                {
                    List<MetadataProto.Layout> layouts = layoutDao.getByTable(table, request.getLayoutVersion(),
                            request.getPermissionRange());
                    if (layouts == null || layouts.isEmpty())
                    {
                        headerBuilder.setErrorCode(METADATA_LAYOUT_NOT_FOUND).setErrorMsg("layout of version '" +
                                request.getLayoutVersion() + "' for table '" +
                                request.getSchemaName() + "." + request.getTableName() + "' with permission '" +
                                request.getPermissionRange().name() + "' not found");
                    } else if (layouts.size() != 1)
                    {
                        headerBuilder.setErrorCode(METADATA_LAYOUT_DUPLICATED).setErrorMsg("duplicated layouts found");
                    } else
                    {
                        layout = layouts.get(0);
                    }
                }
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_TABLE_NOT_FOUND).setErrorMsg("table '" +
                        request.getSchemaName() + "." + request.getTableName() + "' not found");
            }
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_SCHEMA_NOT_FOUND).setErrorMsg("schema '" +
                    request.getSchemaName() + "' not found");
        }
        if(layout != null)
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.GetLayoutResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .setLayout(layout).build();
        }
        else
        {
            response = MetadataProto.GetLayoutResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void addLayout (MetadataProto.AddLayoutRequest request, StreamObserver<MetadataProto.AddLayoutResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        if (layoutDao.save(request.getLayout()))
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_ADD_LAYOUT_FAILED).setErrorMsg("add layout failed");
        }

        MetadataProto.AddLayoutResponse response = MetadataProto.AddLayoutResponse.newBuilder()
                .setHeader(headerBuilder.build()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateLayout (MetadataProto.UpdateLayoutRequest request, StreamObserver<MetadataProto.UpdateLayoutResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        if (layoutDao.update(request.getLayout()))
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_UPDATE_LAYOUT_FAILED).setErrorMsg("make sure the layout exists");
        }

        MetadataProto.UpdateLayoutResponse response = MetadataProto.UpdateLayoutResponse.newBuilder()
                .setHeader(headerBuilder.build()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void createPath(MetadataProto.CreatePathRequest request, StreamObserver<MetadataProto.CreatePathResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        if (this.pathDao.insert(request.getPath()) > 0)
        {
            headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_ADD_PATH_FAILED).setErrorMsg("create path failed");
        }

        MetadataProto.CreatePathResponse response = MetadataProto.CreatePathResponse.newBuilder()
                .setHeader(headerBuilder).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getPaths(MetadataProto.GetPathsRequest request, StreamObserver<MetadataProto.GetPathsResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        MetadataProto.GetPathsResponse.Builder responseBuilder = MetadataProto.GetPathsResponse.newBuilder();
        if (request.hasLayoutId())
        {
            List<MetadataProto.Path> paths = this.pathDao.getAllByLayoutId(request.getLayoutId());
            if (paths != null)
            {
                headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
                responseBuilder.addAllPaths(paths).setHeader(headerBuilder);
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_GET_PATHS_FAILED).setErrorMsg("get paths by layout id failed");
                responseBuilder.setHeader(headerBuilder);
            }
        }
        else if (request.hasRangeId())
        {
            List<MetadataProto.Path> paths = this.pathDao.getAllByRangeId(request.getRangeId());
            if (paths != null)
            {
                headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
                responseBuilder.addAllPaths(paths).setHeader(headerBuilder);
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_GET_PATHS_FAILED).setErrorMsg("get paths by range id failed");
                responseBuilder.setHeader(headerBuilder);
            }
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_GET_PATHS_FAILED).setErrorMsg("request does not have layout id or range id");
            responseBuilder.setHeader(headerBuilder);
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updatePath(MetadataProto.UpdatePathRequest request, StreamObserver<MetadataProto.UpdatePathResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        MetadataProto.Path path = MetadataProto.Path.newBuilder()
                .setId(request.getPathId()).setUri(request.getUri()).setIsCompact(request.getIsCompact()).build();
        if (this.pathDao.update(path))
        {
            headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_UPDATE_PATH_FAILED).setErrorMsg("update path failed");
        }

        MetadataProto.UpdatePathResponse response = MetadataProto.UpdatePathResponse.newBuilder()
                .setHeader(headerBuilder).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void deletePaths(MetadataProto.DeletePathsRequest request, StreamObserver<MetadataProto.DeletePathsResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        if (this.pathDao.deleteByIds(request.getPathIdsList()))
        {
            headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_DELETE_PATHS_FAILED).setErrorMsg("delete paths failed");
        }

        MetadataProto.DeletePathsResponse response = MetadataProto.DeletePathsResponse.newBuilder()
                .setHeader(headerBuilder).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void createPeerPath(MetadataProto.CreatePeerPathRequest request, StreamObserver<MetadataProto.CreatePeerPathResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        if (this.peerPathDao.insert(request.getPeerPath()) > 0)
        {
            headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_ADD_PEER_PATH_FAILED).setErrorMsg("create peer path failed");
        }

        MetadataProto.CreatePeerPathResponse response = MetadataProto.CreatePeerPathResponse.newBuilder()
                .setHeader(headerBuilder).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getPeerPaths(MetadataProto.GetPeerPathsRequest request, StreamObserver<MetadataProto.GetPeerPathsResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        MetadataProto.GetPeerPathsResponse.Builder responseBuilder = MetadataProto.GetPeerPathsResponse.newBuilder();
        if (request.hasPathId())
        {
            List<MetadataProto.PeerPath> peerPaths = this.peerPathDao.getAllByPathId(request.getPathId());
            if (peerPaths != null)
            {
                headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
                responseBuilder.addAllPeerPaths(peerPaths).setHeader(headerBuilder);
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_GET_PEER_PATHS_FAILED).setErrorMsg("get peer paths by path id failed");
                responseBuilder.setHeader(headerBuilder);
            }
        }
        else if (request.hasPeerId())
        {
            List<MetadataProto.PeerPath> peerPaths = this.peerPathDao.getAllByPeerId(request.getPeerId());
            if (peerPaths != null)
            {
                headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
                responseBuilder.addAllPeerPaths(peerPaths).setHeader(headerBuilder);
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_GET_PEER_PATHS_FAILED).setErrorMsg("get peer paths by peer id failed");
                responseBuilder.setHeader(headerBuilder);
            }
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_GET_PEER_PATHS_FAILED).setErrorMsg("request does not have path id or peer id");
            responseBuilder.setHeader(headerBuilder);
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updatePeerPath(MetadataProto.UpdatePeerPathRequest request, StreamObserver<MetadataProto.UpdatePeerPathResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        MetadataProto.PeerPath peerPath = MetadataProto.PeerPath.newBuilder()
                .setId(request.getPeerPathId()).setUri(request.getUri())
                .addAllColumns(request.getColumnsList()).build();
        if (this.peerPathDao.update(peerPath))
        {
            headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_UPDATE_PEER_PATH_FAILED).setErrorMsg("update peer path failed");
        }

        MetadataProto.UpdatePeerPathResponse response = MetadataProto.UpdatePeerPathResponse.newBuilder()
                .setHeader(headerBuilder).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void deletePeerPaths(MetadataProto.DeletePeerPathsRequest request, StreamObserver<MetadataProto.DeletePeerPathsResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        if (this.peerPathDao.deleteByIds(request.getPeerPathIdsList()))
        {
            headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_DELETE_PEER_PATHS_FAILED).setErrorMsg("delete peer paths failed");
        }

        MetadataProto.DeletePeerPathsResponse response = MetadataProto.DeletePeerPathsResponse.newBuilder()
                .setHeader(headerBuilder).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void createPeer(MetadataProto.CreatePeerRequest request, StreamObserver<MetadataProto.CreatePeerResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        if (this.peerDao.insert(request.getPeer()))
        {
            headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_ADD_PEER_FAILED).setErrorMsg("create peer failed");
        }

        MetadataProto.CreatePeerResponse response = MetadataProto.CreatePeerResponse.newBuilder()
                .setHeader(headerBuilder).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getPeer(MetadataProto.GetPeerRequest request, StreamObserver<MetadataProto.GetPeerResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        MetadataProto.GetPeerResponse.Builder responseBuilder = MetadataProto.GetPeerResponse.newBuilder();
        if (request.hasId())
        {
            MetadataProto.Peer peer = this.peerDao.getById(request.getId());
            if (peer != null)
            {
                headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
                responseBuilder.setPeer(peer).setHeader(headerBuilder);
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_GET_PEER_FAILED).setErrorMsg("get peer by id failed");
                responseBuilder.setHeader(headerBuilder);
            }
        }
        else if (request.hasName())
        {
            MetadataProto.Peer peer = this.peerDao.getByName(request.getName());
            if (peer != null)
            {
                headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
                responseBuilder.setPeer(peer).setHeader(headerBuilder);
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_GET_PEER_FAILED).setErrorMsg("get peer by name failed");
                responseBuilder.setHeader(headerBuilder);
            }
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_GET_PEER_FAILED).setErrorMsg("request does not have id or name");
            responseBuilder.setHeader(headerBuilder);
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updatePeer(MetadataProto.UpdatePeerRequest request, StreamObserver<MetadataProto.UpdatePeerResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        if (this.peerDao.update(request.getPeer()))
        {
            headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_UPDATE_PEER_FAILED).setErrorMsg("update peer failed");
        }

        MetadataProto.UpdatePeerResponse response = MetadataProto.UpdatePeerResponse.newBuilder()
                .setHeader(headerBuilder).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void deletePeer(MetadataProto.DeletePeerRequest request, StreamObserver<MetadataProto.DeletePeerResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        if (request.hasId())
        {
            if (this.peerDao.deleteById(request.getId()))
            {
                headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_DELETE_PEER_FAILED).setErrorMsg("delete peer by id failed");
            }
        }
        else if (request.hasName())
        {
            if (this.peerDao.deleteByName(request.getName()))
            {
                headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_DELETE_PEER_FAILED).setErrorMsg("delete peer by name failed");
            }
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_DELETE_PEER_FAILED).setErrorMsg("request does not have id or name");
        }

        MetadataProto.DeletePeerResponse response = MetadataProto.DeletePeerResponse.newBuilder()
                .setHeader(headerBuilder).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getColumns(MetadataProto.GetColumnsRequest request, StreamObserver<MetadataProto.GetColumnsResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        MetadataProto.GetColumnsResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        List<MetadataProto.Column> columns = null;
        if(schema != null)
        {
            MetadataProto.Table table = tableDao.getByNameAndSchema(request.getTableName(), schema);
            if (table != null)
            {
                columns = columnDao.getByTable(table, request.getWithStatistics());
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_TABLE_NOT_FOUND).setErrorMsg("table '" +
                        request.getSchemaName() + "." + request.getTableName() + "' not found");
            }
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_SCHEMA_NOT_FOUND).setErrorMsg("schema '" +
                    request.getSchemaName() + "' not found");
        }
        if(columns != null && columns.isEmpty() == false)
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.GetColumnsResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .addAllColumns(columns).build();
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_COLUMN_NOT_FOUND).setErrorMsg("columns of table '" +
                    request.getSchemaName() + "." + request.getTableName() + "' not found");
            response = MetadataProto.GetColumnsResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateColumn (MetadataProto.UpdateColumnRequest request, StreamObserver<MetadataProto.UpdateColumnResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        boolean res = columnDao.update(request.getColumn());

        if (res)
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_UPDATE_COLUMN_FAILED).setErrorMsg("make sure the column exists");
        }

        MetadataProto.UpdateColumnResponse response = MetadataProto.UpdateColumnResponse.newBuilder()
                .setHeader(headerBuilder.build()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void createSchema(MetadataProto.CreateSchemaRequest request,
                             StreamObserver<MetadataProto.CreateSchemaResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        MetadataProto.Schema schema= MetadataProto.Schema.newBuilder()
        .setName(request.getSchemaName())
        .setDesc(request.getSchemaDesc()).build();
        if (schemaDao.exists(schema))
        {
            headerBuilder.setErrorCode(METADATA_SCHEMA_EXIST).setErrorMsg("schema '" +
                    request.getSchemaName() + "' already exist");
        }
        else
        {
            if (schemaDao.insert(schema))
            {
                headerBuilder.setErrorCode(0).setErrorMsg("");
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_ADD_SCHEMA_FAILED).setErrorMsg("failed to add schema");
            }
        }
        MetadataProto.CreateSchemaResponse response = MetadataProto.CreateSchemaResponse.newBuilder()
                .setHeader(headerBuilder.build()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void dropSchema(MetadataProto.DropSchemaRequest request,
                           StreamObserver<MetadataProto.DropSchemaResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        if (schemaDao.deleteByName(request.getSchemaName()))
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_DELETE_SCHEMA_FAILED).setErrorMsg("failed to delete schema '" +
                    request.getSchemaName() + "'");
        }
        MetadataProto.DropSchemaResponse response = MetadataProto.DropSchemaResponse.newBuilder()
                .setHeader(headerBuilder.build()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void createTable(MetadataProto.CreateTableRequest request,
                            StreamObserver<MetadataProto.CreateTableResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        MetadataProto.Table table = MetadataProto.Table.newBuilder()
        .setName(request.getTableName())
        .setType("user")
        .setStorageScheme(request.getStorageScheme())
        .setSchemaId(schema.getId()).build();
        if (tableDao.exists(table))
        {
            headerBuilder.setErrorCode(METADATA_TABLE_EXIST).setErrorMsg("table '" +
                    request.getSchemaName() + "." + request.getTableName() + "' already exist");
        }
        else
        {
            List<MetadataProto.Column> columns = request.getColumnsList();
            /**
             * Issue #196:
             * Check the data types before inserting the table into metadata.
             */
            boolean typesValid = true;
            String invalidType = "";
            for (MetadataProto.Column column : columns)
            {
                if (!TypeDescription.isValid(column.getType()))
                {
                    typesValid = false;
                    invalidType = column.getType();
                    break;
                }
            }
            if (typesValid)
            {
                if (tableDao.insert(table))
                {
                    // to get table id from database.
                    table = tableDao.getByNameAndSchema(table.getName(), schema);
                    if (columns.size() == columnDao.insertBatch(table, columns))
                    {
                        // Issue #437: TODO - use real transaction timestamp for schema version.
                        MetadataProto.SchemaVersion schemaVersion = buildInitSchemaVersion(table.getId(), columns, 0);
                        long schemaVersionId = schemaVersionDao.insert(schemaVersion);
                        if (schemaVersionId > 0)
                        {
                            MetadataProto.Layout layout = buildInitLayout(table.getId(), schemaVersionId, columns);
                            long layoutId = layoutDao.insert(layout);
                            if (layoutId > 0)
                            {
                                List<MetadataProto.Path> orderedPaths = buildInitPaths(layoutId, layout.getVersion(),
                                        request.getBasePathUrisList(), false);
                                boolean allSuccess = true;
                                for (MetadataProto.Path orderedPath : orderedPaths)
                                {
                                    if (pathDao.insert(orderedPath) <= 0)
                                    {
                                        allSuccess = false;
                                        break;
                                    }
                                }
                                if (allSuccess)
                                {
                                    List<MetadataProto.Path> compactPaths = buildInitPaths(layoutId, layout.getVersion(),
                                            request.getBasePathUrisList(), true);
                                    for (MetadataProto.Path compactPath : compactPaths)
                                    {
                                        if (pathDao.insert(compactPath) <= 0)
                                        {
                                            allSuccess = false;
                                            break;
                                        }
                                    }
                                    if (allSuccess)
                                    {
                                        headerBuilder.setErrorCode(SUCCESS).setErrorMsg("");
                                    }
                                    else
                                    {
                                        headerBuilder.setErrorCode(METADATA_ADD_PATH_FAILED)
                                                .setErrorMsg("failed to add compact paths for table '" +
                                                        request.getSchemaName() + "." + request.getTableName() + "'");
                                    }
                                }
                                else
                                {
                                    headerBuilder.setErrorCode(METADATA_ADD_PATH_FAILED)
                                            .setErrorMsg("failed to add ordered paths for table '" +
                                                    request.getSchemaName() + "." + request.getTableName() + "'");
                                }
                            }
                            else
                            {
                                headerBuilder.setErrorCode(METADATA_ADD_SCHEMA_VERSION_FAILED)
                                        .setErrorMsg("failed to add layout for table '" +
                                                request.getSchemaName() + "." + request.getTableName() + "'");
                            }
                        }
                        else
                        {
                            headerBuilder.setErrorCode(METADATA_ADD_SCHEMA_VERSION_FAILED)
                                    .setErrorMsg("failed to add schema version for table '" +
                                    request.getSchemaName() + "." + request.getTableName() + "'");
                        }
                    } else
                    {
                        headerBuilder.setErrorCode(METADATA_ADD_COLUMNS_FAILED).setErrorMsg(
                                "failed to add columns to table '" + request.getSchemaName() + "." + request.getTableName() + "'");
                    }
                    if (headerBuilder.getErrorCode() != SUCCESS)
                    {
                        // cascade delete the inconsistent states in metadata
                        tableDao.deleteByNameAndSchema(table.getName(), schema);
                    }
                } else
                {
                    headerBuilder.setErrorCode(METADATA_ADD_TABLE_FAILED).setErrorMsg("failed to add table '" +
                            request.getSchemaName() + "." + request.getTableName() + "'");
                }
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_UNKNOWN_DATA_TYPE).setErrorMsg(
                        "unknown data type '" + invalidType + "'");
            }
        }

        MetadataProto.CreateTableResponse response = MetadataProto.CreateTableResponse.newBuilder()
                .setHeader(headerBuilder.build()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void dropTable(MetadataProto.DropTableRequest request, StreamObserver<MetadataProto.DropTableResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        if (tableDao.deleteByNameAndSchema(request.getTableName(), schema))
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_DELETE_TABLE_FAILED).setErrorMsg("failed to delete table '" +
                    request.getSchemaName() + "." + request.getTableName() + "'");
        }
        MetadataProto.DropTableResponse response = MetadataProto.DropTableResponse.newBuilder()
                .setHeader(headerBuilder.build()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void existTable(MetadataProto.ExistTableRequest request, StreamObserver<MetadataProto.ExistTableResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        MetadataProto.ExistTableResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        if (schema == null)
        {
            /**
             * Issue #183:
             * We firstly check the existence of the schema.
             */
            schema = MetadataProto.Schema.newBuilder().setId(-1).setName(request.getSchemaName()).build();
            if (!schemaDao.exists(schema))
            {
                headerBuilder.setErrorCode(0).setErrorMsg("");
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_SCHEMA_NOT_FOUND).setErrorMsg(
                        "failed to get schema '" + request.getSchemaName() + "'");
            }
            response = MetadataProto.ExistTableResponse.newBuilder()
                    .setExists(false).setHeader(headerBuilder.build()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }
        MetadataProto.Table table = MetadataProto.Table.newBuilder()
        .setId(-1)
        .setName(request.getTableName())
        .setSchemaId(schema.getId()).build();

        if (tableDao.exists(table))
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.ExistTableResponse.newBuilder()
                    .setExists(true).setHeader(headerBuilder.build()).build();
        }
        else
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.ExistTableResponse.newBuilder()
                    .setExists(false).setHeader(headerBuilder.build()).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void existSchema(MetadataProto.ExistSchemaRequest request, StreamObserver<MetadataProto.ExistSchemaResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        MetadataProto.Schema schema = MetadataProto.Schema.newBuilder()
                .setId(-1).setName(request.getSchemaName()).build();
        MetadataProto.ExistSchemaResponse response;
        if (schemaDao.exists(schema))
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.ExistSchemaResponse.newBuilder()
                    .setExists(true).setHeader(headerBuilder.build()).build();
        }
        else
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.ExistSchemaResponse.newBuilder()
                    .setExists(false).setHeader(headerBuilder.build()).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void createView(MetadataProto.CreateViewRequest request, StreamObserver<MetadataProto.CreateViewResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        MetadataProto.View view = MetadataProto.View.newBuilder()
                .setName(request.getViewName())
                .setType("user")
                .setData(request.getViewData())
                .setSchemaId(schema.getId()).build();
        if (viewDao.exists(view))
        {
            if (request.getUpdateIfExists())
            {
                if (viewDao.update(view))
                {
                    headerBuilder.setErrorCode(0).setErrorMsg("");
                }
                else
                {
                    headerBuilder.setErrorCode(METADATA_ADD_VIEW_FAILED).setErrorMsg("failed to update view '" +
                            request.getSchemaName() + "." + request.getViewName() + "'");
                }
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_VIEW_EXIST).setErrorMsg("view '" +
                        request.getSchemaName() + "." + request.getViewName() + "' already exist");
            }
        }
        else
        {
            if (viewDao.insert(view))
            {
                headerBuilder.setErrorCode(0).setErrorMsg("");
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_ADD_VIEW_FAILED).setErrorMsg("failed to add view '" +
                        request.getSchemaName() + "." + request.getViewName() + "'");
            }
        }

        MetadataProto.CreateViewResponse response = MetadataProto.CreateViewResponse.newBuilder()
                .setHeader(headerBuilder.build()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void existView(MetadataProto.ExistViewRequest request, StreamObserver<MetadataProto.ExistViewResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        MetadataProto.ExistViewResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        if (schema == null)
        {
            schema = MetadataProto.Schema.newBuilder().setId(-1).setName(request.getSchemaName()).build();
            if (!schemaDao.exists(schema))
            {
                headerBuilder.setErrorCode(0).setErrorMsg("");
            }
            else
            {
                headerBuilder.setErrorCode(METADATA_SCHEMA_NOT_FOUND).setErrorMsg(
                        "failed to get schema '" + request.getSchemaName() + "'");
            }
            response = MetadataProto.ExistViewResponse.newBuilder()
                    .setExists(false).setHeader(headerBuilder.build()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }
        MetadataProto.View view = MetadataProto.View.newBuilder()
                .setId(-1)
                .setName(request.getViewName())
                .setSchemaId(schema.getId()).build();

        if (viewDao.exists(view))
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.ExistViewResponse.newBuilder()
                    .setExists(true).setHeader(headerBuilder.build()).build();
        }
        else
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.ExistViewResponse.newBuilder()
                    .setExists(false).setHeader(headerBuilder.build()).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getViews(MetadataProto.GetViewsRequest request, StreamObserver<MetadataProto.GetViewsResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        MetadataProto.ResponseHeader header;
        MetadataProto.GetViewsResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        List<MetadataProto.View> views;

        if(schema != null)
        {
            views = viewDao.getBySchema(schema);
            /**
             * views.isEmpty() is normal for a schema without any views.
             */
            if (views == null)
            {
                header = headerBuilder.setErrorCode(METADATA_VIEW_NOT_FOUND)
                        .setErrorMsg("metadata server failed to get views").build();
                response = MetadataProto.GetViewsResponse.newBuilder()
                        .setHeader(header).build();
            }
            else
            {
                header = headerBuilder.setErrorCode(0).setErrorMsg("").build();
                response = MetadataProto.GetViewsResponse.newBuilder()
                        .setHeader(header)
                        .addAllViews(views).build();
            }
        }
        else
        {
            header = headerBuilder.setErrorCode(METADATA_SCHEMA_NOT_FOUND).setErrorMsg("schema '" +
                    request.getSchemaName() + "' not found").build();
            response = MetadataProto.GetViewsResponse.newBuilder().setHeader(header).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getView(MetadataProto.GetViewRequest request, StreamObserver<MetadataProto.GetViewResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        MetadataProto.ResponseHeader header;
        MetadataProto.GetViewResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        MetadataProto.View view;

        if(schema != null)
        {
            view = viewDao.getByNameAndSchema(request.getViewName(), schema);
            if (view == null)
            {
                header = headerBuilder.setErrorCode(METADATA_VIEW_NOT_FOUND)
                        .setErrorMsg("metadata server failed to get view '" +
                                request.getSchemaName() + "." + request.getViewName() + "'").build();
                response = MetadataProto.GetViewResponse.newBuilder()
                        .setHeader(header).build();
            }
            else
            {
                header = headerBuilder.setErrorCode(0).setErrorMsg("").build();
                response = MetadataProto.GetViewResponse.newBuilder()
                        .setHeader(header).setView(view).build();
            }
        }
        else
        {
            header = headerBuilder.setErrorCode(METADATA_SCHEMA_NOT_FOUND).setErrorMsg("schema '" +
                    request.getSchemaName() + "' not found").build();
            response = MetadataProto.GetViewResponse.newBuilder().setHeader(header).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void dropView(MetadataProto.DropViewRequest request, StreamObserver<MetadataProto.DropViewResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());

        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        if (viewDao.deleteByNameAndSchema(request.getViewName(), schema))
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(METADATA_DELETE_VIEW_FAILED).setErrorMsg("failed to delete view '" +
                    request.getSchemaName() + "." + request.getViewName() + "'");
        }
        MetadataProto.DropViewResponse response = MetadataProto.DropViewResponse.newBuilder()
                .setHeader(headerBuilder.build()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
