package io.pixelsdb.pixels.presto.impl;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Schema;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.presto.PixelsColumnHandle;
import io.pixelsdb.pixels.presto.PixelsTable;
import io.pixelsdb.pixels.presto.PixelsTableHandle;
import io.pixelsdb.pixels.presto.PixelsTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.List;

import static io.pixelsdb.pixels.presto.PixelsTypeManager.getColumnType;

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

    public PixelsTable getTable(String connectorId, String schemaName, String tableName) throws MetadataException
    {
        return getTable(connectorId, schemaName, tableName, "");
    }

    public PixelsTable getTable(String connectorId, String schemaName, String tableName, String path) throws MetadataException
    {
        PixelsTableHandle tableHandle = new PixelsTableHandle(connectorId, schemaName, tableName, path);

        TupleDomain<ColumnHandle> constraint = TupleDomain.all();
        PixelsTableLayoutHandle tableLayout = new PixelsTableLayoutHandle(tableHandle);
        tableLayout.setConstraint(constraint);
        List<PixelsColumnHandle> columns = new ArrayList<PixelsColumnHandle>();
        List<ColumnMetadata> columnsMetadata = new ArrayList<ColumnMetadata>();
        List<Column> columnsList = metadataService.getColumns(schemaName, tableName);
        for (int i = 0; i < columnsList.size(); i++) {
            Column c = columnsList.get(i);
            Type columnType = getColumnType(c);
            if (columnType == null)
            {
                log.error("columnType is not defined.");
            }
            String name = c.getName();
            ColumnMetadata columnMetadata = new ColumnMetadata(name, columnType);
            PixelsColumnHandle pixelsColumnHandle = new PixelsColumnHandle(connectorId, name, columnType, "", i);

            columns.add(pixelsColumnHandle);
            columnsMetadata.add(columnMetadata);
        }
        PixelsTable table = new PixelsTable(tableHandle, tableLayout, columns, columnsMetadata);
        return table;
    }

    public List<PixelsColumnHandle> getTableColumn(String connectorId, String schemaName, String tableName) throws MetadataException
    {
        List<PixelsColumnHandle> columns = new ArrayList<PixelsColumnHandle>();
        List<Column> columnsList = metadataService.getColumns(schemaName, tableName);
        for (int i = 0; i < columnsList.size(); i++) {
            Column c = columnsList.get(i);
            Type columnType = getColumnType(c);
            if (columnType == null)
            {
                log.error("columnType is not defined.");
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

    public boolean createTable (String schemaName, String tableName, List<Column> columns) throws MetadataException
    {
        return metadataService.createTable(schemaName, tableName, columns);
    }

    public boolean dropTable (String schemaName, String tableName) throws MetadataException
    {
        return metadataService.dropTable(schemaName, tableName);
    }

    public boolean existTable (String schemaName, String tableName) throws MetadataException
    {
        return metadataService.existTable(schemaName, tableName);
    }
}
