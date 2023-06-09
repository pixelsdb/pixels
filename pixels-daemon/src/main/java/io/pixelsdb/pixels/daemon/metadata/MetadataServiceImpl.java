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

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.MetadataServiceGrpc;
import io.pixelsdb.pixels.daemon.metadata.dao.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static io.pixelsdb.pixels.common.error.ErrorCode.*;

/**
 * Created at: 19-4-16
 * Author: hank
 */
public class MetadataServiceImpl extends MetadataServiceGrpc.MetadataServiceImplBase
{
    private static Logger log = LogManager.getLogger(MetadataServiceImpl.class);

    private SchemaDao schemaDao = DaoFactory.Instance().getSchemaDao();
    private TableDao tableDao = DaoFactory.Instance().getTableDao();
    private ColumnDao columnDao = DaoFactory.Instance().getColumnDao();
    private LayoutDao layoutDao = DaoFactory.Instance().getLayoutDao();
    private ViewDao viewDao = DaoFactory.Instance().getViewDao();

    public MetadataServiceImpl () { }

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
            headerBuilder.setErrorCode(METADATA_UPDATE_COUMN_FAILED).setErrorMsg("make sure the column exists");
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
                        headerBuilder.setErrorCode(0).setErrorMsg("");
                    } else
                    {
                        tableDao.deleteByNameAndSchema(table.getName(), schema);
                        headerBuilder.setErrorCode(METADATA_ADD_COUMNS_FAILED).setErrorMsg(
                                "failed to add columns to table '" +
                                        request.getSchemaName() + "." + request.getTableName() + "'");
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
