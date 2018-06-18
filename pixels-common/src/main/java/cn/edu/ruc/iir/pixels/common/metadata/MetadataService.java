package cn.edu.ruc.iir.pixels.common.metadata;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by hank on 18-6-17.
 */
public class MetadataService
{
    private String host;
    private int port;

    public MetadataService(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    public List<Column> getColumns(String schemaName, String tableName) throws MetadataException
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
            throw new MetadataException("can not get columns from metadata", e);
        }
        return columns != null ? columns : new ArrayList<>();
    }

    public List<Layout> getLayouts(String schemaName, String tableName) throws MetadataException
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
            throw new MetadataException("can not get layouts from metadata", e);
        }
        return layouts != null ? layouts : new ArrayList<>();
    }

    public List<Table> getTables(String schemaName) throws MetadataException
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
            throw new MetadataException("can not get tables from metadata", e);
        }
        return tables != null ? tables : new ArrayList<>();
    }

    public List<Schema> getSchemas() throws MetadataException
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
            throw new MetadataException("can not get schemas from metadata", e);
        }
        return schemas != null ? schemas : new ArrayList<>();
    }
}
