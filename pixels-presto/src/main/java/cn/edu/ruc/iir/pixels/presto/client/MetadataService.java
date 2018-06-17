package cn.edu.ruc.iir.pixels.presto.client;

import cn.edu.ruc.iir.pixels.common.metadata.*;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.presto.impl.PixelsPrestoConfig;
import com.alibaba.fastjson.JSON;
import com.facebook.presto.spi.PrestoException;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_CLIENT_SERIVCE_ERROR;

/**
 * Created by hank on 18-6-17.
 */
public class MetadataService
{
    private String host;
    private int port;
    private static Logger logger = Logger.get(MetadataService.class);

    @Inject
    public MetadataService(PixelsPrestoConfig config)
    {
        ConfigFactory configFactory = config.getFactory();
        this.host = configFactory.getProperty("metadata.server.host");
        this.port = Integer.parseInt(configFactory.getProperty("metadata.server.port"));
    }

    public List<Column> getColumns(String schemaName, String tableName)
    {
        List<Column> columns;
        String token = UUID.randomUUID().toString();
        ReqParams params = new ReqParams(Action.getColumns.toString());
        params.setParam("tableName", tableName);
        params.setParam("schemaName", schemaName);
        MetadataClient client = new MetadataClient(params, token);
        try
        {
            client.connect(port, host);
            while (true)
            {
                String res = client.getResponse().get(token);
                if (res != null)
                {
                    columns = JSON.parseArray(res, Column.class);
                    break;
                }
            }
        } catch (Exception e)
        {
            logger.error(e, "can not get columns from metadata");
            throw new PrestoException(PIXELS_CLIENT_SERIVCE_ERROR, e);
        }
        return columns != null ? columns : new ArrayList<>();
    }

    public List<Layout> getLayouts(String schemaName, String tableName)
    {
        List<Layout> layouts;
        String token = UUID.randomUUID().toString();
        ReqParams params = new ReqParams(Action.getLayouts.toString());
        params.setParam("tableName", tableName);
        params.setParam("schemaName", schemaName);
        MetadataClient client = new MetadataClient(params, token);
        try
        {
            client.connect(port, host);
            while (true)
            {
                String res = client.getResponse().get(token);
                if (res != null)
                {
                    layouts = JSON.parseArray(res, Layout.class);
                    break;
                }
            }
        } catch (Exception e)
        {
            logger.error(e, "can not get layouts from metadata");
            throw new PrestoException(PIXELS_CLIENT_SERIVCE_ERROR, e);
        }
        return layouts != null ? layouts : new ArrayList<>();
    }

    public List<Table> getTables(String schemaName)
    {
        List<Table> tables;
        String token = UUID.randomUUID().toString();
        ReqParams params = new ReqParams(Action.getTables.toString());
        params.setParam("schemaName", schemaName);
        MetadataClient client = new MetadataClient(params, token);
        try
        {
            client.connect(port, host);
            while (true)
            {
                String res = client.getResponse().get(token);
                if (res != null)
                {
                    tables = JSON.parseArray(res, Table.class);
                    break;
                }
            }
        } catch (Exception e)
        {
            logger.error(e, "can not get tables from metadata");
            throw new PrestoException(PIXELS_CLIENT_SERIVCE_ERROR, e);
        }
        return tables != null ? tables : new ArrayList<>();
    }

    public List<Schema> getSchemas()
    {
        List<Schema> schemas;
        String token = UUID.randomUUID().toString();
        ReqParams params = new ReqParams(Action.getSchemas.toString());
        MetadataClient client = new MetadataClient(params, token);
        try
        {
            client.connect(port, host);
            while (true)
            {
                String res = client.getResponse().get(token);
                if (res != null)
                {
                    schemas = JSON.parseArray(res, Schema.class);
                    break;
                }
            }
        } catch (Exception e)
        {
            logger.error(e, "can not get schemas from metadata");
            throw new PrestoException(PIXELS_CLIENT_SERIVCE_ERROR, e);
        }
        return schemas != null ? schemas : new ArrayList<>();
    }
}
