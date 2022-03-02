/*
 * Copyright 2018-2019 PixelsDB.
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
package io.pixelsdb.pixels.presto.impl;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Schema;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.presto.PixelsColumnHandle;
import io.pixelsdb.pixels.presto.PixelsTypeManager;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hank on 18-6-18.
 */
public class PixelsMetadataProxy
{
    private static final Logger log = Logger.get(PixelsMetadataProxy.class);
    private final MetadataService metadataService;

    @Inject
    public PixelsMetadataProxy(PixelsPrestoConfig config)
    {
        ConfigFactory configFactory = config.getConfigFactory();
        String host = configFactory.getProperty("metadata.server.host");
        int port = Integer.parseInt(configFactory.getProperty("metadata.server.port"));
        this.metadataService = new MetadataService(host, port);
        Runtime.getRuntime().addShutdownHook(new Thread( () ->
        {
            try
            {
                this.metadataService.shutdown();
            } catch (InterruptedException e)
            {
                throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR,
                        "Failed to shutdown metadata service (client).");
            }
        }));
    }

    public List<String> getSchemaNames() throws MetadataException
    {
        List<String> schemaList = new ArrayList<String>();
        List<Schema> schemas = metadataService.getSchemas();
        for (Schema s : schemas) {
            schemaList.add(s.getName());
        }
        return schemaList;
    }

    public List<String> getTableNames(String schemaName) throws MetadataException
    {
        List<String> tableList = new ArrayList<String>();
        List<Table> tables = metadataService.getTables(schemaName);
        for (Table t : tables) {
            tableList.add(t.getName());
        }
        return tableList;
    }

    public Table getTable(String schemaName, String tableName) throws MetadataException
    {
        return metadataService.getTable(schemaName, tableName);
    }

    public List<PixelsColumnHandle> getTableColumn(String connectorId, String schemaName, String tableName) throws MetadataException
    {
        List<PixelsColumnHandle> columns = new ArrayList<PixelsColumnHandle>();
        List<Column> columnsList = metadataService.getColumns(schemaName, tableName);
        for (int i = 0; i < columnsList.size(); i++) {
            Column c = columnsList.get(i);
            Type columnType = PixelsTypeManager.getColumnType(c);
            if (columnType == null)
            {
                throw new PrestoException(PixelsErrorCode.PIXELS_METASTORE_ERROR,
                        "columnType '" + c.getType() + "' is not defined.");
            }
            String name = c.getName();
            PixelsColumnHandle pixelsColumnHandle = new PixelsColumnHandle(connectorId, name, columnType, "", i);
            columns.add(pixelsColumnHandle);
        }
        return columns;
    }

    public List<Layout> getDataLayouts (String schemaName, String tableName) throws MetadataException
    {
        return metadataService.getLayouts(schemaName, tableName);
    }

    public boolean createSchema (String schemaName) throws MetadataException
    {
        return metadataService.createSchema(schemaName);
    }

    public boolean dropSchema (String schemaName) throws MetadataException
    {
        return metadataService.dropSchema(schemaName);
    }

    public boolean createTable (String schemaName, String tableName, String storageScheme,
                                List<Column> columns) throws MetadataException
    {
        return metadataService.createTable(schemaName, tableName, storageScheme, columns);
    }

    public boolean dropTable (String schemaName, String tableName) throws MetadataException
    {
        return metadataService.dropTable(schemaName, tableName);
    }

    public boolean existTable (String schemaName, String tableName) throws MetadataException
    {
        return metadataService.existTable(schemaName, tableName);
    }

    public boolean createView (String schemaName, String viewName, String viewData) throws MetadataException
    {
        return metadataService.createView(schemaName, viewName, viewData);
    }

    public boolean dropView (String schemaName, String viewName) throws MetadataException
    {
        return metadataService.dropView(schemaName, viewName);
    }

    public boolean existView (String schemaName, String viewName) throws MetadataException
    {
        return metadataService.existView(schemaName, viewName);
    }

    public boolean existSchema (String schemaName) throws MetadataException
    {
        return metadataService.existSchema(schemaName);
    }
}
