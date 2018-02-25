package cn.edu.ruc.iir.pixels.presto.client;

import cn.edu.ruc.iir.pixels.common.ConfigFactory;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Table;
import com.alibaba.fastjson.JSON;
import com.facebook.presto.spi.ColumnMetadata;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.Metadata
 * @ClassName: MetadataService
 * @Description: get metaData
 * @author: tao
 * @date: Create in 2018-01-21 21:54
 **/
public class MetadataService {
    static ConfigFactory config = ConfigFactory.Instance();
    static String host = config.getProperty("metadata.server.host");
    static int port = Integer.valueOf(config.getProperty("metadata.server.port"));


    public static List<Column> getColumnsBySchemaNameAndTblName(String schemaName, String tableName) {
        List<Column> columns = new ArrayList<Column>();
        MetadataClient client = new MetadataClient(Action.getColumns.toString());
        try {
            try {
                client.connect(port, host, tableName + "&" + schemaName);
            } catch (Exception e) {
                e.printStackTrace();
            }
            while (true) {
                int count = client.getQueue().size();
                if (count > 0) {
                    String res = client.getQueue().poll();
                    columns = JSON.parseArray(res, Column.class);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return columns;
    }

    public static List<Layout> getLayoutsByTblName(String tableName) {
        List<Layout> layouts = new ArrayList<>();
        MetadataClient client = new MetadataClient(Action.getLayouts.toString());
        try {
            try {
                client.connect(port, host, tableName);
            } catch (Exception e) {
                e.printStackTrace();
            }
            while (true) {
                int count = client.getQueue().size();
                if (count > 0) {
                    String res = client.getQueue().poll();
                    layouts = JSON.parseArray(res, Layout.class);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return layouts;
    }

    public static List<Table> getTablesBySchemaName(String schemaName) {
        List<Table> tables = new ArrayList<>();
        MetadataClient client = new MetadataClient(Action.getTables.toString());
        try {
            try {
                client.connect(port, host, schemaName);
            } catch (Exception e) {
                e.printStackTrace();
            }
            while (true) {
                int count = client.getQueue().size();
                if (count > 0) {
                    String res = client.getQueue().poll();
                    tables = JSON.parseArray(res, Table.class);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tables;
    }

    public static List<Schema> getSchemas() {
        List<Schema> schemas = new ArrayList<>();
        MetadataClient client = new MetadataClient(Action.getSchemas.toString());
        try {
            try {
                client.connect(port, host, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
            while (true) {
                int count = client.getQueue().size();
                if (count > 0) {
                    String res = client.getQueue().poll();
                    schemas = JSON.parseArray(res, Schema.class);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return schemas;
    }
}
