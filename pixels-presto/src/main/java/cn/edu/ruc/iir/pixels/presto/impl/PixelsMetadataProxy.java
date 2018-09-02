package cn.edu.ruc.iir.pixels.presto.impl;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Table;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.presto.PixelsColumnHandle;
import cn.edu.ruc.iir.pixels.presto.PixelsTable;
import cn.edu.ruc.iir.pixels.presto.PixelsTableHandle;
import cn.edu.ruc.iir.pixels.presto.PixelsTableLayoutHandle;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

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
        ConfigFactory configFactory = config.getFactory();
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
        List<String> tablelist = new ArrayList<String>();
        List<Table> tables = metadataService.getTables(schemaName);
        for (Table t : tables) {
            tablelist.add(t.getName());
        }
        return tablelist;
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
            Type columnType = null;
            String name = c.getName();
            String type = c.getType().toLowerCase();
            if (type.equals("int")) {
                columnType = INTEGER;
            } else if (type.equals("bigint") || type.equals("long")) {
                columnType = BIGINT;
            } else if (type.equals("double")|| type.equals("float")) {
                columnType = DOUBLE;
            } else if (type.equals("varchar") || type.equals("string")) {
                columnType = VARCHAR;
            } else if (type.equals("boolean")) {
                columnType = BOOLEAN;
            } else if (type.equals("timestamp")) {
                columnType = TIMESTAMP;
            }
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
            Type columnType = null;
            String name = c.getName();
            String type = c.getType().toLowerCase();
            if (type.equals("int")) {
                columnType = INTEGER;
            } else if (type.equals("bigint")) {
                columnType = BIGINT;
            } else if (type.equals("double")|| type.equals("float")) {
                columnType = DOUBLE;
            } else if (type.equals("varchar") || type.equals("string")) {
                columnType = VARCHAR;
            } else if (type.equals("boolean")) {
                columnType = BOOLEAN;
            } else if (type.equals("timestamp")) {
                columnType = TIMESTAMP;
            } else {
                System.out.println("columnType is not defined.");
            }
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
