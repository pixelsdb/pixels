package cn.edu.ruc.iir.pixels.presto.client;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Table;
import cn.edu.ruc.iir.pixels.presto.exception.PixelsUriExceotion;
import cn.edu.ruc.iir.pixels.presto.impl.PixelsPrestoConfig;
import com.alibaba.fastjson.JSON;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.Metadata
 * @ClassName: MetadataService
 * @Description: get metaData
 * @author: tao
 * @date: Create in 2018-01-21 21:54
 **/
public class MetadataService {
    private String host;
    private int port;
    private static Logger logger = Logger.get(MetadataService.class);

    @Inject
    public MetadataService(PixelsPrestoConfig config) throws PixelsUriExceotion
    {
        String uri = config.getMetadataServerUri();
        if (uri.startsWith("pixels://") == false || uri.contains(":") == false)
        {
            throw new PixelsUriExceotion("invalid pixels uri: " + uri);
        }
        String[] splits = uri.substring(9).split(":");
        this.host = splits[0];
        this.port = Integer.parseInt(splits[1]);
    }

    public List<Column> getColumnsBySchemaNameAndTblName(String schemaName, String tableName) {
        List<Column> columns = new ArrayList<>();
        String token = UUID.randomUUID().toString();
        MetadataClient client = new MetadataClient(Action.getColumns.toString(), token);
        try {
            try {
                client.connect(port, host, tableName + "&" + schemaName);
            } catch (Exception e) {
                e.printStackTrace();
            }
            while (true) {
                String res = client.getMap().get(token);
                if (res != null) {
                    columns = JSON.parseArray(res, Column.class);
                    client.getMap().remove(token);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return columns;
    }

    public List<Layout> getLayoutsByTblName(String tableName) {
        List<Layout> layouts = new ArrayList<>();
        String token = UUID.randomUUID().toString();
        MetadataClient client = new MetadataClient(Action.getLayouts.toString(), token);
        try {
            try {
                client.connect(port, host, tableName);
            } catch (Exception e) {
                e.printStackTrace();
            }
            while (true) {
                String res = client.getMap().get(token);
                if (res != null) {
                    layouts = JSON.parseArray(res, Layout.class);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return layouts;
    }

    public List<Table> getTablesBySchemaName(String schemaName) {
        List<Table> tables = new ArrayList<>();
        String token = UUID.randomUUID().toString();
        MetadataClient client = new MetadataClient(Action.getTables.toString(), token);
        try {
            try {
                client.connect(port, host, schemaName);
            } catch (Exception e) {
                e.printStackTrace();
            }
            while (true) {
                String res = client.getMap().get(token);
                if (res != null) {
                    tables = JSON.parseArray(res, Table.class);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tables;
    }

    public List<Schema> getSchemas() {
        List<Schema> schemas = new ArrayList<>();
        String token = UUID.randomUUID().toString();
        MetadataClient client = new MetadataClient(Action.getSchemas.toString(), token);
        try {
            try {
                client.connect(port, host, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
            while (true) {
                String res = client.getMap().get(token);
                if (res != null) {
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
