package cn.edu.ruc.iir.pixels.presto.impl;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Table;
import cn.edu.ruc.iir.pixels.presto.PixelsColumnHandle;
import cn.edu.ruc.iir.pixels.presto.PixelsTable;
import cn.edu.ruc.iir.pixels.presto.PixelsTableHandle;
import cn.edu.ruc.iir.pixels.presto.PixelsTableLayoutHandle;
import cn.edu.ruc.iir.pixels.presto.client.MetadataService;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.impl
 * @ClassName: PixelsMetadataReader
 * @Description: Read metadata
 * @author: tao
 * @date: Create in 2018-01-20 11:15
 **/
public class PixelsMetadataReader {
    private static final Logger log = Logger.get(PixelsMetadataReader.class);
    private static PixelsMetadataReader instance = null;
    private MetadataService metadataService = MetadataService.Instance();

    @Inject
    private PixelsMetadataReader() {

    }

    public static PixelsMetadataReader Instance() {
        if (instance == null) {
            instance = new PixelsMetadataReader();
        }
        return instance;
    }

    public List<String> getSchemaNames() {
        List<String> schemaList = new ArrayList<String>();
        List<Schema> schemas = metadataService.getSchemas();
        for (Schema s : schemas) {
            schemaList.add(s.getSchName());
        }
        return schemaList;
    }

    public List<String> getTableNames(String schemaName) {
        List<String> tablelist = new ArrayList<String>();
        List<Table> tables = metadataService.getTablesBySchemaName(schemaName);
        for (Table t : tables) {
            tablelist.add(t.getTblName());
        }
        return tablelist;
    }

    public PixelsTable getTable(String connectorId, String schemaName, String tableName) {
        return getTable(connectorId, schemaName, tableName, "");
    }

    public PixelsTable getTable(String connectorId, String schemaName, String tableName, String path) {
        PixelsTableHandle tableHandle = new PixelsTableHandle(connectorId, schemaName, tableName, path);

        TupleDomain<ColumnHandle> constraint = TupleDomain.all();
        PixelsTableLayoutHandle tableLayout = new PixelsTableLayoutHandle(tableHandle, constraint);

        List<PixelsColumnHandle> columns = new ArrayList<PixelsColumnHandle>();
        List<ColumnMetadata> columnsMetadata = new ArrayList<ColumnMetadata>();
        List<Column> columnsList = metadataService.getColumnsBySchemaNameAndTblName(schemaName, tableName);
        for (Column c : columnsList) {
            Type columnType = null;
            String name = c.getColName();
            String type = c.getColType().toLowerCase();
            if (type.equals("int")) {
                columnType = INTEGER;
            } else if (type.equals("double")) {
                columnType = DOUBLE;
            } else if (type.equals("varchar")) {
                columnType = VARCHAR;
            }
            ColumnMetadata columnMetadata = new ColumnMetadata(name, columnType);
            PixelsColumnHandle pixelsColumnHandle = new PixelsColumnHandle(connectorId, name, columnType, "", 0);

            columns.add(pixelsColumnHandle);
            columnsMetadata.add(columnMetadata);
        }
        PixelsTable table = new PixelsTable(tableHandle, tableLayout, columns, columnsMetadata);
        return table;
    }

    public List<PixelsColumnHandle> getTableColumn(String connectorId, String schemaName, String tableName) {

        List<PixelsColumnHandle> columns = new ArrayList<PixelsColumnHandle>();
        List<Column> columnsList = metadataService.getColumnsBySchemaNameAndTblName(schemaName, tableName);
        for (Column c : columnsList) {
            Type columnType = null;
            String name = c.getColName();
            String type = c.getColType().toLowerCase();
            if (type.equals("int")) {
                columnType = INTEGER;
            } else if (type.equals("double")) {
                columnType = DOUBLE;
            } else if (type.equals("varchar")) {
                columnType = VARCHAR;
            }
            PixelsColumnHandle pixelsColumnHandle = new PixelsColumnHandle(connectorId, name, columnType, "", 0);
            columns.add(pixelsColumnHandle);
        }
        return columns;
    }

    public PixelsTableLayoutHandle getTableLayout(String connectorId, String schemaName, String tableName) {
        PixelsTableHandle tableHandle = new PixelsTableHandle(connectorId, schemaName, tableName, "no meaning path");
        TupleDomain<ColumnHandle> constraint = TupleDomain.all();
        PixelsTableLayoutHandle tableLayout = new PixelsTableLayoutHandle(tableHandle, constraint);
        return tableLayout;
    }
}
