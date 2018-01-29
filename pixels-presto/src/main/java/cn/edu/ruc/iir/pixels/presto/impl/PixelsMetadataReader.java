package cn.edu.ruc.iir.pixels.presto.impl;

import cn.edu.ruc.iir.pixels.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.metadata.domain.Table;
import cn.edu.ruc.iir.pixels.presto.PixelsColumnHandle;
import cn.edu.ruc.iir.pixels.presto.PixelsTable;
import cn.edu.ruc.iir.pixels.presto.PixelsTableHandle;
import cn.edu.ruc.iir.pixels.presto.PixelsTableLayoutHandle;
import cn.edu.ruc.iir.pixels.presto.client.TimeClient;
import com.alibaba.fastjson.JSON;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;

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

    public List<String> getSchemaNames() {
        List<String> schemaList = new ArrayList<String>();
        List<Schema> schemas = new ArrayList<>();
        TimeClient client = new TimeClient("getSchemaNames");
        try {
            new Thread(() -> {
                try {
                    client.connect(18888, "127.0.0.1");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
            while (true) {
                int count = client.getQueue().size();
                if (count > 0) {
                    String res = client.getQueue().poll();
                    schemas = JSON.parseArray(res, Schema.class);
                    break;
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("Schemas: " + schemaList.size());
        for (Schema s : schemas) {
            schemaList.add(s.getSchName());
            log.info("getSchName: " + s.toString());
        }
        return schemaList;
    }

    public List<String> getTableNames(String schemaName) {
        log.info("Function getTableNames() -> schemaName: " + schemaName);
        List<String> tablelist = new ArrayList<String>();
        List<Table> tables = new ArrayList<>();
        TimeClient client = new TimeClient("getTableNames");
        try {
            new Thread(() -> {
                try {
                    client.connect(18888, "127.0.0.1");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
            while (true) {
                int count = client.getQueue().size();
                if (count > 0) {
                    String res = client.getQueue().poll();
                    tables = JSON.parseArray(res, Table.class);
                    break;
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("Tables: " + tablelist.size());
        for (Table t : tables) {
            tablelist.add(t.getTblName());
            log.info("getTblName: " + t.toString());
        }
        return tablelist;
    }

    public static PixelsTable getTable(String connectorId, String schemaName, String tableName) {
        PixelsTableHandle tableHandle = new PixelsTableHandle(connectorId, "default", "test", "pixels/db/default/test");

        TupleDomain<ColumnHandle> constraint = TupleDomain.all();
        PixelsTableLayoutHandle tableLayout = new PixelsTableLayoutHandle(tableHandle, constraint);

        List<PixelsColumnHandle> columns = new ArrayList<PixelsColumnHandle>();
        PixelsColumnHandle pixelsColumnHandle = new PixelsColumnHandle(connectorId, "id", createUnboundedVarcharType(), "", 0);
        columns.add(pixelsColumnHandle);
        PixelsColumnHandle pixelsColumnHandle1 = new PixelsColumnHandle(connectorId, "x", createUnboundedVarcharType(), "", 1);
        columns.add(pixelsColumnHandle1);
        PixelsColumnHandle pixelsColumnHandle2 = new PixelsColumnHandle(connectorId, "y", createUnboundedVarcharType(), "", 2);
        columns.add(pixelsColumnHandle2);

        List<ColumnMetadata> columnsMetadata = MetadataService.getColumnMetadata();

        PixelsTable table = new PixelsTable(tableHandle, tableLayout, columns, columnsMetadata);
        return table;
    }

    public static PixelsTableLayoutHandle getTableLayout(String connectorId, String schemaName, String tableName) {
        PixelsTableHandle tableHandle = new PixelsTableHandle(connectorId, "default", "test", "pixels/db/default/test");

        TupleDomain<ColumnHandle> constraint = TupleDomain.all();
        PixelsTableLayoutHandle tableLayout = new PixelsTableLayoutHandle(tableHandle, constraint);
        return tableLayout;
    }
}
