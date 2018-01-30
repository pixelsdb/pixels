package cn.edu.ruc.iir.pixels.presto.client;

import cn.edu.ruc.iir.pixels.common.ConfigFactory;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Catalog;
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
    static String host = config.getProperty("host");
    static int port = Integer.valueOf(config.getProperty("port"));


    public static List<ColumnMetadata> getColumnMetadata() {
        List<ColumnMetadata> list = new ArrayList<ColumnMetadata>();
        ColumnMetadata c1 = new ColumnMetadata("id", createUnboundedVarcharType(), "", false);
        list.add(c1);
        ColumnMetadata c2 = new ColumnMetadata("x", createUnboundedVarcharType(), "", false);
        list.add(c2);
        ColumnMetadata c3 = new ColumnMetadata("y", createUnboundedVarcharType(), "", false);
        list.add(c3);
        return list;
    }

    public static List<Catalog> getCatalogsByTblName(String tableName) {
        List<Catalog> catalogs = new ArrayList<>();
        MetadataClient client = new MetadataClient(Action.getCatalogs.toString());
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
                    catalogs = JSON.parseArray(res, Catalog.class);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return catalogs;
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
