package cn.edu.ruc.iir.pixels.common.metadata;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.domain.*;
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
                Object res = client.getResponse().get(token);
                if (res != null)
                {
                    columns = (List<Column>) res;
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
                Object res = client.getResponse().get(token);
                if (res != null)
                {
                    layouts = (List<Layout>) res;
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
                Object res = client.getResponse().get(token);
                if (res != null)
                {
                    tables = (List<Table>) res;
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
                Object res = client.getResponse().get(token);
                if (res != null)
                {
                    schemas = (List<Schema>) res;
                    break;
                }
            }
        } catch (Exception e)
        {
            throw new MetadataException("can not get schemas from metadata", e);
        }
        return schemas != null ? schemas : new ArrayList<>();
    }

    public boolean createSchema (String schemaName) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        String token = UUID.randomUUID().toString();
        ReqParams params = new ReqParams(Action.createSchema.toString());
        params.setParam("schemaName", schemaName);
        return submitCheckRequest(params, token).equals("success");
    }

    public boolean dropSchema (String schemaName) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        String token = UUID.randomUUID().toString();
        ReqParams params = new ReqParams(Action.dropSchema.toString());
        params.setParam("schemaName", schemaName);
        return submitCheckRequest(params, token).equals("success");
    }

    public boolean createTable (String schemaName, String tableName, List<Column> columns) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        assert tableName != null && !tableName.isEmpty();
        assert columns != null && !columns.isEmpty();

        String token = UUID.randomUUID().toString();
        ReqParams params = new ReqParams(Action.createTable.toString());
        params.setParam("schemaName", schemaName);
        params.setParam("tableName", tableName);
        params.setParam("columns", JSON.toJSONString(columns));
        return submitCheckRequest(params, token).equals("success");
    }

    public boolean dropTable (String schemaName, String tableName) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        assert tableName != null && !tableName.isEmpty();

        String token = UUID.randomUUID().toString();
        ReqParams params = new ReqParams(Action.dropTable.toString());
        params.setParam("schemaName", schemaName);
        params.setParam("tableName", tableName);
        return submitCheckRequest(params, token).equals("success");
    }

    public boolean existTable (String schemaName, String tableName) throws MetadataException
    {
        assert schemaName != null && !schemaName.isEmpty();
        assert tableName != null && !tableName.isEmpty();

        String token = UUID.randomUUID().toString();
        ReqParams params = new ReqParams(Action.existTable.toString());
        params.setParam("schemaName", schemaName);
        params.setParam("tableName", tableName);
        return submitCheckRequest(params, token).equals("true");
    }

    private String submitCheckRequest (ReqParams params, String token) throws MetadataException
    {
        MetadataClient client = new MetadataClient(params, token);
        try
        {
            client.connect(port, host);
            while (true)
            {
                Object res = client.getResponse().get(token);
                if (res != null)
                {
                    return String.valueOf(res);
                }
            }
        } catch (Exception e)
        {
            throw new MetadataException("can not create schema in metadata", e);
        }
    }
}
