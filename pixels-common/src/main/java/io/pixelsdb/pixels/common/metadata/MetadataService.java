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

import com.google.common.collect.ImmutableList;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.layout.IndexFactory;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.server.HostAddress;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.MetadataServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.pixelsdb.pixels.common.error.ErrorCode.*;

/**
 * @author hank
 * @create 2018-06-17
 * @update 2019-04 adapt to GRPC.
 */
public class MetadataService
{
    private static final Logger logger = LogManager.getLogger(MetadataService.class);
    private static final MetadataService defaultInstance;
    private static final Map<HostAddress, MetadataService> otherInstances = new HashMap<>();

    static
    {
        String metadataHost = ConfigFactory.Instance().getProperty("metadata.server.host");
        int metadataPort = Integer.parseInt(ConfigFactory.Instance().getProperty("metadata.server.port"));
        defaultInstance = new MetadataService(metadataHost, metadataPort);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    defaultInstance.shutdown();
                    for (MetadataService otherMetadataService : otherInstances.values())
                    {
                        otherMetadataService.shutdown();
                    }
                    otherInstances.clear();
                } catch (InterruptedException e)
                {
                    logger.error("failed to shut down metadata service", e);
                }
            }
        }));
    }

    /**
     * Get the default metadata service instance connecting to the metadata host:port configured in
     * PIXELS_HOME/etc/pixels.properties. This default instance will be automatically shut down when the process
     * is terminating, no need to call {@link #shutdown()} (although it is idempotent) manually.
     * @return
     */
    public static MetadataService Instance()
    {
        return defaultInstance;
    }

    /**
     * This method should only be used to connect to a metadata server that is not configured through
     * PIXELS_HOME/etc/pixels.properties. <b>No need</b> to manually shut down the returned metadata service.
     * @param host the host name of the metadata server
     * @param port the port of the metadata server
     * @return the created metadata service instance
     */
    public static MetadataService CreateInstance(String host, int port)
    {
        HostAddress address = HostAddress.fromParts(host, port);
        MetadataService metadataService = otherInstances.get(address);
        if (metadataService != null)
        {
            return metadataService;
        }
        metadataService = new MetadataService(host, port);
        otherInstances.put(address, metadataService);
        return metadataService;
    }

    private final ManagedChannel channel;
    private final MetadataServiceGrpc.MetadataServiceBlockingStub stub;
    private boolean isShutDown;

    private MetadataService(String host, int port)
    {
        assert (host != null);
        assert (port > 0 && port <= 65535);
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = MetadataServiceGrpc.newBlockingStub(channel);
        this.isShutDown = false;
    }

    private synchronized void shutdown() throws InterruptedException
    {
        if (!this.isShutDown)
        {
            // Wait for at most 5 seconds, this should be enough to shut down an RPC client.
            this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            this.isShutDown = true;
        }
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
        if (!response.getHeader().getToken().equals(token))
        {
            throw new MetadataException("response token does not match.");
        }
        return true;
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
            if (!response.getHeader().getToken().equals(token))
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

    public boolean existSchema(String schemaName) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();

        String token = UUID.randomUUID().toString();
        MetadataProto.ExistSchemaRequest request = MetadataProto.ExistSchemaRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).build();
        MetadataProto.ExistSchemaResponse response = this.stub.existSchema(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new MetadataException("failed to check table existence. error code="
                    + response.getHeader().getErrorCode()
                    + ", error message=" + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new MetadataException("response token does not match.");
        }
        return response.getExists();
    }

    public boolean dropSchema(String schemaName) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();

        List<Table> tables = getTables(schemaName);
        if (tables != null)
        {
            // Issue #630: drop cached splits and projections indexes.
            for (Table table : tables)
            {
                IndexFactory.Instance().dropSplitsIndex(schemaName, table.getName());
                IndexFactory.Instance().dropProjectionsIndex(schemaName, table.getName());
            }
        }

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

    public boolean createTable(String schemaName, String tableName, Storage.Scheme storageScheme,
                               List<String> basePathUris, List<Column> columns) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        assert tableName != null && !tableName.isEmpty();
        assert storageScheme != null;
        assert columns != null && !columns.isEmpty();

        String token = UUID.randomUUID().toString();
        List<MetadataProto.Column> columnList = new ArrayList<>();
        for (Column column : columns)
        {
            columnList.add(MetadataProto.Column.newBuilder()
                    .setId(column.getId()).setName(column.getName())
                    .setType(column.getType().replaceAll("\\s", ""))
                    .build()); // no need to set table id.
        }
        MetadataProto.CreateTableRequest request = MetadataProto.CreateTableRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).setTableName(tableName).setStorageScheme(storageScheme.name())
                .addAllBasePathUris(basePathUris).addAllColumns(columnList).build();
        MetadataProto.CreateTableResponse response = this.stub.createTable(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new MetadataException("failed to create table. error code=" + response.getHeader().getErrorCode()
                    + ", error message=" + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new MetadataException("response token does not match.");
        }
        return true;
    }

    public Table getTable(String schemaName, String tableName) throws MetadataException
    {
        return getTable(schemaName, tableName, false);
    }

    public Table getTable(String schemaName, String tableName, boolean withLayouts) throws MetadataException
    {
        Table table = null;
        String token = UUID.randomUUID().toString();
        MetadataProto.GetTableRequest request = MetadataProto.GetTableRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).setTableName(tableName).setWithLayouts(withLayouts).build();
        try
        {
            MetadataProto.GetTableResponse response = this.stub.getTable(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            table = new Table(response.getTable(), response.getLayoutsList());
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get tables from metadata", e);
        }
        return table;
    }

    public List<Table> getTables(String schemaName) throws MetadataException
    {
        ImmutableList.Builder<Table> tablesBuilder = ImmutableList.builder();
        String token = UUID.randomUUID().toString();
        MetadataProto.GetTablesRequest request = MetadataProto.GetTablesRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).build();
        try
        {
            MetadataProto.GetTablesResponse response = this.stub.getTables(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            response.getTablesList().forEach(table -> tablesBuilder.add(new Table(table)));
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get tables from metadata", e);
        }
        return tablesBuilder.build();
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
            throw new MetadataException("failed to check table existence. error code="
                    + response.getHeader().getErrorCode()
                    + ", error message=" + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new MetadataException("response token does not match.");
        }
        return response.getExists();
    }

    public boolean dropTable(String schemaName, String tableName) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        assert tableName != null && !tableName.isEmpty();

        // Issue #630: drop cached splits and projections indexes.
        IndexFactory.Instance().dropSplitsIndex(schemaName, tableName);
        IndexFactory.Instance().dropProjectionsIndex(schemaName, tableName);

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
        if (!response.getHeader().getToken().equals(token))
        {
            throw new MetadataException("response token does not match.");
        }
        return true;
    }

    public void updateRowCount(String schemaName, String tableName, long rowCount) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.UpdateRowCountRequest request = MetadataProto.UpdateRowCountRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).setTableName(tableName).setRowCount(rowCount).build();
        try
        {
            MetadataProto.UpdateRowCountResponse response = this.stub.updateRowCount(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to update table row count", e);
        }
    }

    public boolean createView(String schemaName, String viewName, String viewData, boolean updateIfExists) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        assert viewName != null && !viewName.isEmpty();
        assert viewData != null && !viewData.isEmpty();

        String token = UUID.randomUUID().toString();
        MetadataProto.CreateViewRequest request = MetadataProto.CreateViewRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build()).setSchemaName(schemaName)
                .setViewName(viewName).setViewData(viewData).setUpdateIfExists(updateIfExists).build();
        MetadataProto.CreateViewResponse response = this.stub.createView(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new MetadataException("failed to create view. error code=" + response.getHeader().getErrorCode()
                    + ", error message=" + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new MetadataException("response token does not match.");
        }
        return true;
    }

    public boolean existView(String schemaName, String viewName) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        assert viewName != null && !viewName.isEmpty();

        String token = UUID.randomUUID().toString();
        MetadataProto.ExistViewRequest request = MetadataProto.ExistViewRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).setViewName(viewName).build();
        MetadataProto.ExistViewResponse response = this.stub.existView(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new MetadataException("failed to check view existence. error code="
                    + response.getHeader().getErrorCode()
                    + ", error message=" + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new MetadataException("response token does not match.");
        }
        return response.getExists();
    }

    public boolean dropView(String schemaName, String viewName) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        assert viewName != null && !viewName.isEmpty();

        String token = UUID.randomUUID().toString();
        MetadataProto.DropViewRequest request = MetadataProto.DropViewRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).setViewName(viewName).build();
        MetadataProto.DropViewResponse response = this.stub.dropView(request);
        if (response.getHeader().getErrorCode() != 0)
        {
            throw new MetadataException("failed to drop view. error code=" + response.getHeader().getErrorCode()
                    + ", error message=" + response.getHeader().getErrorMsg());
        }
        if (!response.getHeader().getToken().equals(token))
        {
            throw new MetadataException("response token does not match.");
        }
        return true;
    }

    public View getView(String schemaName, String viewName, boolean returnNullIfNotExists) throws MetadataException
    {
        View view = null;
        String token = UUID.randomUUID().toString();
        MetadataProto.GetViewRequest request = MetadataProto.GetViewRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).setViewName(viewName).build();
        try
        {
            MetadataProto.GetViewResponse response = this.stub.getView(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                if (returnNullIfNotExists && response.getHeader().getErrorCode() == METADATA_VIEW_NOT_FOUND)
                {
                    return null;
                }
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            view = new View(response.getView());
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get views from metadata", e);
        }
        return view;
    }

    public List<View> getViews(String schemaName) throws MetadataException
    {
        List<View> views = new ArrayList<>();
        String token = UUID.randomUUID().toString();
        MetadataProto.GetViewsRequest request = MetadataProto.GetViewsRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).build();
        try
        {
            MetadataProto.GetViewsResponse response = this.stub.getViews(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            response.getViewsList().forEach(view -> views.add(new View(view)));
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get views from metadata", e);
        }
        return views;
    }

    public List<Column> getColumns(String schemaName, String tableName, boolean getStatistics) throws MetadataException
    {
        List<Column> columns = new ArrayList<>();
        String token = UUID.randomUUID().toString();
        MetadataProto.GetColumnsRequest request = MetadataProto.GetColumnsRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName).setTableName(tableName).setWithStatistics(getStatistics).build();
        try
        {
            MetadataProto.GetColumnsResponse response = this.stub.getColumns(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
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

    public void updateColumn(Column column) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.Column columnPb = column.toProto();
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
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to update column in metadata", e);
        }
    }

    /**
     * The ordered and compact paths of the layout are not added, they need to be added separately.
     * @param layout the new layout
     * @return true if success, false otherwise
     * @throws MetadataException
     */
    public boolean addLayout(Layout layout) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.AddLayoutRequest request = MetadataProto.AddLayoutRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setLayout(layout.toProto()).build();
        try
        {
            MetadataProto.AddLayoutResponse response = this.stub.addLayout(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
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

    /**
     * Get the readable layouts of a table.
     * @param schemaName the schema name
     * @param tableName the table name
     * @return the readable layouts of the table
     * @throws MetadataException
     */
    public List<Layout> getLayouts(String schemaName, String tableName) throws MetadataException
    {
        ImmutableList.Builder<Layout> layoutsBuilder = ImmutableList.builder();
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
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            response.getLayoutsList().forEach(layout -> layoutsBuilder.add(new Layout(layout)));
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get layouts from metadata", e);
        }
        return layoutsBuilder.build();
    }

    public Layout getLayout(String schemaName, String tableName, int version) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.GetLayoutRequest request = MetadataProto.GetLayoutRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setLayoutVersion(version)
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
                .setLayoutVersion(-1)
                .setPermissionRange(MetadataProto.GetLayoutRequest.PermissionRange.ALL).build();
        return internalGetLayout(request);
    }

    /**
     * Get the writable layout of the table. Each table should have exact one writable layout.
     * @param schemaName schema name of the table
     * @param tableName name of the table
     * @return the writable layout
     * @throws MetadataException
     */
    public Layout getWritableLayout(String schemaName, String tableName) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.GetLayoutRequest request = MetadataProto.GetLayoutRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setLayoutVersion(-1)
                .setPermissionRange(MetadataProto.GetLayoutRequest.PermissionRange.READ_WRITE).build();
        return internalGetLayout(request);
    }

    private Layout internalGetLayout(MetadataProto.GetLayoutRequest request) throws MetadataException
    {
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
            if (!response.getHeader().getToken().equals(request.getHeader().getToken()))
            {
                throw new MetadataException("response token does not match.");
            }
            return new Layout(response.getLayout());
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get layouts from metadata", e);
        }
    }

    /**
     * The ordered and compact paths of the layout are not updated, they need to be added separately.
     * @param layout the new layout
     * @return true if success, false otherwise
     * @throws MetadataException
     */
    public boolean updateLayout(Layout layout) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.Layout layoutPb = layout.toProto();
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
            if (!response.getHeader().getToken().equals(token))
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

    public boolean createRangeIndex(RangeIndex rangeIndex) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.CreateRangeIndexRequest request = MetadataProto.CreateRangeIndexRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setRangeIndex(rangeIndex.toProto()).build();
        try
        {
            MetadataProto.CreateRangeIndexResponse response = this.stub.createRangeIndex(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to add range index", e);
        }
        return true;
    }

    /**
     * Get range index by table id.
     * @param tableId the table id
     * @return null if range index is not found for the table id
     * @throws MetadataException
     */
    public RangeIndex getRangeIndex(long tableId) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.GetRangeIndexRequest request = MetadataProto.GetRangeIndexRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setTableId(tableId).build();
        try
        {
            MetadataProto.GetRangeIndexResponse response = this.stub.getRangeIndex(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                if (response.getHeader().getErrorCode() == METADATA_RANGE_INDEX_NOT_FOUND)
                {
                    /**
                     * Issue #658:
                     * return null if range index is not found, this is useful for clients
                     * as they can hardly deal with error code.
                     */
                    return null;
                }
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            return new RangeIndex(response.getRangeIndex());
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get range index", e);
        }
    }

    public boolean updateRangeIndex(RangeIndex rangeIndex) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.UpdateRangeIndexRequest request = MetadataProto.UpdateRangeIndexRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setRangeIndex(rangeIndex.toProto()).build();
        try
        {
            MetadataProto.UpdateRangeIndexResponse response = this.stub.updateRangeIndex(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to update range index", e);
        }
        return true;
    }

    public boolean dropRangeIndex(long tableId) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.DropRangeIndexRequest request = MetadataProto.DropRangeIndexRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setTableId(tableId).build();
        try
        {
            MetadataProto.DropRangeIndexResponse response = this.stub.dropRangeIndex(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to delete range index", e);
        }
        return true;
    }

    public boolean addRange(Range range) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.AddRangeRequest request = MetadataProto.AddRangeRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setRange(range.toProto()).build();
        try
        {
            MetadataProto.AddRangeResponse response = this.stub.addRange(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to add range", e);
        }
        return true;
    }

    public Range getRange(long rangeId) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.GetRangeRequest request = MetadataProto.GetRangeRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setRangeId(rangeId).build();
        try
        {
            MetadataProto.GetRangeResponse response = this.stub.getRange(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                if (response.getHeader().getErrorCode() == METADATA_RANGE_NOT_FOUNT)
                {
                    /**
                     * Issue #658:
                     * return null if range is not found, this is useful for clients
                     * as they can hardly deal with error code.
                     */
                    return null;
                }
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            return new Range(response.getRange());
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get range", e);
        }
    }

    public List<Range> getRanges(long rangeIndexId) throws MetadataException
    {
        List<Range> ranges = new ArrayList<>();
        String token = UUID.randomUUID().toString();
        MetadataProto.GetRangesRequest request = MetadataProto.GetRangesRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setRangeIndexId(rangeIndexId).build();
        try
        {
            MetadataProto.GetRangesResponse response = this.stub.getRanges(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            response.getRangesList().forEach(range -> ranges.add(new Range(range)));
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get ranges from metadata", e);
        }
        return ranges;
    }

    public boolean deleteRange(long rangeId) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.DeleteRangeRequest request = MetadataProto.DeleteRangeRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token).build())
                .setRangeId(rangeId).build();
        try
        {
            MetadataProto.DeleteRangeResponse response = this.stub.deleteRange(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to delete range", e);
        }
        return true;
    }

    public boolean addPath(Path path) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.AddPathRequest request = MetadataProto.AddPathRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token))
                .setPath(path.toProto()).build();
        try
        {
            MetadataProto.AddPathResponse response = this.stub.addPath(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to add path", e);
        }
        return false;
    }

    public List<Path> getPaths(long layoutOrRangeId, boolean isLayoutId) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.GetPathsRequest.Builder requestBuilder = MetadataProto.GetPathsRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token));
        if (isLayoutId)
        {
            requestBuilder.setLayoutId(layoutOrRangeId);
        }
        else
        {
            requestBuilder.setRangeId(layoutOrRangeId);
        }
        try
        {
            MetadataProto.GetPathsResponse response = this.stub.getPaths(requestBuilder.build());
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            return Path.convertPaths(response.getPathsList());
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get paths", e);
        }
    }

    public boolean updatePath(Path path) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.UpdatePathRequest request = MetadataProto.UpdatePathRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token))
                .setPath(path.toProto()).build();
        try
        {
            MetadataProto.UpdatePathResponse response = this.stub.updatePath(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to update path", e);
        }
        return false;
    }

    public boolean deletePaths(List<Long> pathIds) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.DeletePathsRequest request = MetadataProto.DeletePathsRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token))
                .addAllPathIds(pathIds).build();
        try
        {
            MetadataProto.DeletePathsResponse response = this.stub.deletePaths(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to delete paths", e);
        }
        return false;
    }

    public boolean addFiles(Collection<File> files) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.AddFilesRequest request = MetadataProto.AddFilesRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token))
                .addAllFiles(files.stream().map(File::toProto).collect(Collectors.toList())).build();
        try
        {
            MetadataProto.AddFilesResponse response = this.stub.addFiles(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to add file", e);
        }
        return false;
    }

    /**
     * Get the file id of the file given the full uri path containing the storage scheme prefix.
     * @param filePathUri the file path uri containing the storage scheme prefix
     * @return the id of the file, valid file id should be a positive integer
     * @throws MetadataException if failed to request the file id
     */
    public long getFileId(String filePathUri) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.GetFileIdRequest request = MetadataProto.GetFileIdRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token))
                .setFilePathUri(filePathUri).build();
        try
        {
            MetadataProto.GetFileIdResponse response = this.stub.getFileId(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            return response.getFileId();
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get file id", e);
        }
    }

    public List<File> getFiles(long pathId) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.GetFilesRequest request = MetadataProto.GetFilesRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token)).setPathId(pathId).build();
        try
        {
            MetadataProto.GetFilesResponse response = this.stub.getFiles(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            return File.convertFiles(response.getFilesList());
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get files", e);
        }
    }

    public boolean updateFile(File file) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.UpdateFileRequest request = MetadataProto.UpdateFileRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token))
                .setFile(file.toProto()).build();
        try
        {
            MetadataProto.UpdateFileResponse response = this.stub.updateFile(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to update file", e);
        }
        return false;
    }

    public boolean deleteFiles(List<Long> fileIds) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.DeleteFilesRequest request = MetadataProto.DeleteFilesRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token))
                .addAllFileIds(fileIds).build();
        try
        {
            MetadataProto.DeleteFilesResponse response = this.stub.deleteFiles(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to delete files", e);
        }
        return false;
    }

    public boolean createPeerPath(String uri, List<Column> columns, Path path, Peer peer) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.CreatePeerPathRequest request = MetadataProto.CreatePeerPathRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token))
                .setPeerPath(MetadataProto.PeerPath.newBuilder().setUri(uri)
                        .addAllColumns(Column.revertColumns(columns)).setPathId(path.getId())
                        .setPeerId(peer.getId())).build();
        try
        {
            MetadataProto.CreatePeerPathResponse response = this.stub.createPeerPath(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to create peer path in metadata", e);
        }
        return false;
    }

    public List<PeerPath> getPeerPaths(long pathOrPeerId, boolean isPathId) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.GetPeerPathsRequest.Builder requestBuilder = MetadataProto.GetPeerPathsRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token));
        if (isPathId)
        {
            requestBuilder.setPathId(pathOrPeerId);
        }
        else
        {
            requestBuilder.setPeerId(pathOrPeerId);
        }
        try
        {
            MetadataProto.GetPeerPathsResponse response = this.stub.getPeerPaths(requestBuilder.build());
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            return PeerPath.convertPeerPaths(response.getPeerPathsList());
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get peer paths from metadata", e);
        }
    }

    public boolean updatePeerPath(long peerPathId, String uri, List<Column> columns) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.UpdatePeerPathRequest request = MetadataProto.UpdatePeerPathRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token)).setPeerPathId(peerPathId)
                .setUri(uri).addAllColumns(Column.revertColumns(columns)).build();
        try
        {
            MetadataProto.UpdatePeerPathResponse response = this.stub.updatePeerPath(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to update peer path in metadata", e);
        }
        return false;
    }

    public boolean deletePeerPaths(List<Long> peerPathIds) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.DeletePeerPathsRequest request = MetadataProto.DeletePeerPathsRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token))
                .addAllPeerPathIds(peerPathIds).build();
        try
        {
            MetadataProto.DeletePeerPathsResponse response = this.stub.deletePeerPaths(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to delete peer paths from metadata", e);
        }
        return false;
    }

    public boolean createPeer(String name, String location,
                              String host, int port, Storage.Scheme storageScheme) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.CreatePeerRequest request = MetadataProto.CreatePeerRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token))
                .setPeer(MetadataProto.Peer.newBuilder().setName(name).setLocation(location)
                        .setHost(host).setPort(port).setStorageScheme(storageScheme.name())).build();
        try
        {
            MetadataProto.CreatePeerResponse response = this.stub.createPeer(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to create peer in metadata", e);
        }
        return false;
    }

    public Peer getPeer(int peerId) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.GetPeerRequest request = MetadataProto.GetPeerRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token)).setId(peerId).build();
        try
        {
            MetadataProto.GetPeerResponse response = this.stub.getPeer(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            return new Peer(response.getPeer());
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to create peer in metadata", e);
        }
    }

    public Peer getPeer(String name) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.GetPeerRequest request = MetadataProto.GetPeerRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token)).setName(name).build();
        try
        {
            MetadataProto.GetPeerResponse response = this.stub.getPeer(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
            return new Peer(response.getPeer());
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to get peer in metadata", e);
        }
    }

    public boolean updatePeer(Peer peer) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.UpdatePeerRequest request = MetadataProto.UpdatePeerRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token))
                .setPeer(peer.toProto()).build();
        try
        {
            MetadataProto.UpdatePeerResponse response = this.stub.updatePeer(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to update peer in metadata", e);
        }
        return false;
    }

    public boolean deletePeer(int peerId) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.DeletePeerRequest request = MetadataProto.DeletePeerRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token))
                .setId(peerId).build();
        try
        {
            MetadataProto.DeletePeerResponse response = this.stub.deletePeer(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to delete peer from metadata", e);
        }
        return false;
    }

    public boolean deletePeer(String name) throws MetadataException
    {
        String token = UUID.randomUUID().toString();
        MetadataProto.DeletePeerRequest request = MetadataProto.DeletePeerRequest.newBuilder()
                .setHeader(MetadataProto.RequestHeader.newBuilder().setToken(token))
                .setName(name).build();
        try
        {
            MetadataProto.DeletePeerResponse response = this.stub.deletePeer(request);
            if (response.getHeader().getErrorCode() != 0)
            {
                throw new MetadataException("error code=" + response.getHeader().getErrorCode()
                        + ", error message=" + response.getHeader().getErrorMsg());
            }
            if (!response.getHeader().getToken().equals(token))
            {
                throw new MetadataException("response token does not match.");
            }
        }
        catch (Exception e)
        {
            throw new MetadataException("failed to delete peer from metadata", e);
        }
        return false;
    }
}
