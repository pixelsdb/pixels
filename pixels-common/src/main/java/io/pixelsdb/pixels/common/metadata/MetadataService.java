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
package io.pixelsdb.pixels.common.metadata;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Schema;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.MetadataServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.pixelsdb.pixels.common.error.ErrorCode.METADATA_LAYOUT_NOT_FOUND;

/**
 * Created by hank on 18-6-17.
 * Adapt to GRPC in April 2019.
 */
public class MetadataService
{
    private final ManagedChannel channel;
    private final MetadataServiceGrpc.MetadataServiceBlockingStub stub;

    public MetadataService(String host, int port)
    {
        assert (host != null);
        assert (port > 0 && port <= 65535);
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true).build();
        this.stub = MetadataServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException
    {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public List<Schema> getSchemas() throws MetadataException
    {
        List<Schema> schemas = new ArrayList<>();
        String token = UUID.randomUUID().toString();
        MetadataProto.GetSchemasRequest request = MetadataProto.GetSchemasRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build()).build();
        try
        {
            MetadataProto.GetSchemasResponse response = this.stub.getSchemas(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (response.getHeader().getToken().equals(token) == false)
            {
                throw new MetadataException("response token does not match.");
            }
            response.getSchemasList().forEach(schema -> schemas.add(new Schema(schema)));
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get schemas from metadata", e);
        }
        return schemas;
    }

    public List<Table> getTables(String schemaName) throws MetadataException
    {
        List<Table> tables = new ArrayList<>();
        String token = UUID.randomUUID().toString();
        MetadataProto.GetTablesRequest request = MetadataProto.GetTablesRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).build();
        try
        {
            MetadataProto.GetTablesResponse response = this.stub.getTables(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (response.getHeader().getToken().equals(token) == false)
            {
                throw new MetadataException("response token does not match.");
            }
            response.getTablesList().forEach(table -> tables.add(new Table(table)));
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get tables from metadata", e);
        }
        return tables;
    }

    public List<Column> getColumns(String schemaName, String tableName) throws MetadataException
    {
        List<Column> columns = new ArrayList<>();
        String token = UUID.randomUUID().toString();
        MetadataProto.GetColumnsRequest request = MetadataProto.GetColumnsRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).setTableName(tableName).build();
        try
        {
            MetadataProto.GetColumnsResponse response = this.stub.getColumns(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (response.getHeader().getToken().equals(token) == false)
            {
                throw new MetadataException("response token does not match.");
            }
            response.getColumnsList().forEach(column -> columns.add(new Column(column)));
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get columns from metadata", e);
        }
        return columns;
    }

    public boolean updateColumn(Column column) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.Column columnPb = MetadataProto.Column.newBuilder()
                .setId(column.getId()).setName(column.getName()).setType(column.getType())
                .setSize(column.getSize()).setTableId(column.getTableId()).build();
        MetadataProto.UpdateColumnRequest request = MetadataProto.UpdateColumnRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setColumn(columnPb).build();
        try
        {
            MetadataProto.UpdateColumnResponse response = this.stub.updateColumn(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (response.getHeader().getToken().equals(token) == false)
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to update column in metadata", e);
        }
        return true;
    }

    /**
     * Get the readable layouts of a table.
     * @param schemaName
     * @param tableName
     * @return
     * @throws MetadataException
     */
    public List<Layout> getLayouts(String schemaName, String tableName) throws MetadataException
    {
        List<Layout> layouts = new ArrayList<>();
        String token = UUID.randomUUID().toString();
        MetadataProto.GetLayoutsRequest request = MetadataProto.GetLayoutsRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).setTableName(tableName).build();
        try
        {
            MetadataProto.GetLayoutsResponse response = this.stub.getLayouts(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (response.getHeader().getToken().equals(token) == false)
            {
                throw new MetadataException("response token does not match.");
            }
            response.getLayoutsList().forEach(layout -> layouts.add(new Layout(layout)));
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get layouts from metadata", e);
        }
        return layouts;
    }

    public Layout getLayout(String schemaName, String tableName, int version) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.GetLayoutRequest request = MetadataProto.GetLayoutRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setVersion(version)
                .setPermissionRange(MetadataProto.GetLayoutRequest.PermissionRange.READABLE).build();
        return internalGetLayout(request);
    }

    public Layout getLatestLayout(String schemaName, String tableName) throws MetadataException
    {

        String token = UUID.randomUUID().toString();
        MetadataProto.GetLayoutRequest request = MetadataProto.GetLayoutRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setVersion(-1)
                .setPermissionRange(MetadataProto.GetLayoutRequest.PermissionRange.ALL).build();

        return internalGetLayout(request);
    }

    private Layout internalGetLayout(MetadataProto.GetLayoutRequest request) throws MetadataException
    {
        Layout layout;
        try
        {
            MetadataProto.GetLayoutResponse response = this.stub.getLayout(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                if (response.getHeader().getErrorCode() == METADATA_LAYOUT_NOT_FOUND)
                {
                    /**
                     * Issue #98:
                     * return null if layout is not found, this is useful for clients
                     * as they can hardly deal with error code.
                     */
                    return null;
                }
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (response.getHeader().getToken().equals(request.getHeader().getToken()) == false)
            {
                throw new MetadataException("response token does not match.");
            }
            layout = new Layout(response.getLayout());
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get layouts from metadata", e);
        }
        return layout;
    }

    private MetadataProto.Layout.Permission convertPermission(Layout.Permission permission)
    {
        switch (permission)
        {
            case DISABLED:
                return MetadataProto.Layout.Permission.DISABLED;
            case READ_ONLY:
                return MetadataProto.Layout.Permission.READ_ONLY;
            case READ_WRITE:
                return MetadataProto.Layout.Permission.READ_WRITE;
        }
        return MetadataProto.Layout.Permission.UNRECOGNIZED;
    }

    public boolean updateLayout(Layout layout) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.Layout layoutPb = MetadataProto.Layout.newBuilder()
                .setId(layout.getId()).setPermission(convertPermission(layout.getPermission()))
                .setCreateAt(layout.getCreateAt()).setSplits(layout.getSplits())
                .setOrder(layout.getOrder()).setOrderPath(layout.getOrderPath())
                .setCompact(layout.getCompact()).setCompactPath(layout.getCompactPath())
                .setVersion(layout.getVersion()).setTableId(layout.getTableId()).build();
        MetadataProto.UpdateLayoutRequest request = MetadataProto.UpdateLayoutRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setLayout(layoutPb).build();
        try
        {
            MetadataProto.UpdateLayoutResponse response = this.stub.updateLayout(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (response.getHeader().getToken().equals(token) == false)
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to update layout in metadata", e);
        }
        return true;
    }

    public boolean addLayout(Layout layout) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.Layout layoutPb = MetadataProto.Layout.newBuilder()
                .setId(layout.getId()).setPermission(convertPermission(layout.getPermission()))
                .setCreateAt(layout.getCreateAt()).setSplits(layout.getSplits())
                .setOrder(layout.getOrder()).setOrderPath(layout.getOrderPath())
                .setCompact(layout.getCompact()).setCompactPath(layout.getCompactPath())
                .setVersion(layout.getVersion()).setTableId(layout.getTableId()).build();
        MetadataProto.AddLayoutRequest request = MetadataProto.AddLayoutRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setLayout(layoutPb).build();
        try
        {
            MetadataProto.AddLayoutResponse response = this.stub.addLayout(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (response.getHeader().getToken().equals(token) == false)
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to add layout into metadata", e);
        }
        return true;
    }

    public boolean createSchema(String schemaName) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        String token = UUID.randomUUID().toString();
        MetadataProto.CreateSchemaRequest request = MetadataProto.CreateSchemaRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).setSchemaDesc("Created by Pixels MetadataService").build();
        MetadataProto.CreateSchemaResponse response = this.stub.createSchema(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new MetadataException("failed to create schema. error code=" + response.getHeader().getErrorCode()
                    + ", error message=" + response.getHeader().getErrorMsg());
        }
        if (response.getHeader().getToken().equals(token) == false)
        {
            throw new MetadataException("response token does not match.");
        }
        return true;
    }

    public boolean dropSchema(String schemaName) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        String token = UUID.randomUUID().toString();
        MetadataProto.DropSchemaRequest request = MetadataProto.DropSchemaRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).build();
        MetadataProto.DropSchemaResponse response = this.stub.dropSchema(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new MetadataException("failed to drop schema. error code=" + response.getHeader().getErrorCode()
                    + ", error message=" + response.getHeader().getErrorMsg());
        }
        return true;
    }

    public boolean createTable(String schemaName, String tableName, List<Column> columns) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        assert tableName != null && !tableName.isEmpty();
        assert columns != null && !columns.isEmpty();

        String token = UUID.randomUUID().toString();
        List<MetadataProto.Column> columnList = new ArrayList<>();
        for (Column column : columns)
        {
            columnList.add(MetadataProto.Column.newBuilder()
                    .setId(column.getId()).setName(column.getName()).setType(column.getType())
                    .setSize(column.getSize()).build()); // no need to set table id.
        }
        MetadataProto.CreateTableRequest request = MetadataProto.CreateTableRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).setTableName(tableName).addAllColumns(columnList).build();
        MetadataProto.CreateTableResponse response = this.stub.createTable(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new MetadataException("failed to create table. error code=" + response.getHeader().getErrorCode()
                    + ", error message=" + response.getHeader().getErrorMsg());
        }
        if (response.getHeader().getToken().equals(token) == false)
        {
            throw new MetadataException("response token does not match.");
        }
        return true;
    }

    public boolean dropTable(String schemaName, String tableName) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        assert tableName != null && !tableName.isEmpty();

        String token = UUID.randomUUID().toString();
        MetadataProto.DropTableRequest request = MetadataProto.DropTableRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).setTableName(tableName).build();
        MetadataProto.DropTableResponse response = this.stub.dropTable(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new MetadataException("failed to drop table. error code=" + response.getHeader().getErrorCode()
                    + ", error message=" + response.getHeader().getErrorMsg());
        }
        if (response.getHeader().getToken().equals(token) == false)
        {
            throw new MetadataException("response token does not match.");
        }
        return true;
    }

    public boolean existTable(String schemaName, String tableName) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        assert tableName != null && !tableName.isEmpty();

        String token = UUID.randomUUID().toString();
        MetadataProto.ExistTableRequest request = MetadataProto.ExistTableRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).setTableName(tableName).build();
        MetadataProto.ExistTableResponse response = this.stub.existTable(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new MetadataException("failed to check table. error code=" + response.getHeader().getErrorCode()
                    + ", error message=" + response.getHeader().getErrorMsg());
        }
        if (response.getHeader().getToken().equals(token) == false)
        {
            throw new MetadataException("response token does not match.");
        }
        return response.getExists();
    }
}
