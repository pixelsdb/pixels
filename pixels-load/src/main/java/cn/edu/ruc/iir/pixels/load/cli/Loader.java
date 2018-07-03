package cn.edu.ruc.iir.pixels.load.cli;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Order;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import com.alibaba.fastjson.JSON;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * <p>

 * <p>
 * LOAD -p /home/tao/software/data/pixels/data/ -s /home/tao/software/data/pixels/Test.sql -f hdfs://presto00:9000/po_compare/
 * <br> 1000 columns
 * <p>
 * DDL -s /home/tao/software/data/pixels/test30G_pixels/presto_ddl.sql -d pixels
 * <p>
 * LOAD -p /home/tao/software/data/pixels/test30G_pixels/data/ -s /home/tao/software/data/pixels/test30G_pixels/presto_ddl.sql -f hdfs://presto00:9000/pixels/test30G_pixels/
 * <p>
 * DDL -s /home/tao/software/data/pixels/test30G_pixels/105/presto_ddl.sql -d pixels
 * <p>
 **/
public abstract class Loader
{
    private final String originalDataPath;
    private final String dbName;
    private final String tableName;
    private final int maxRowNum;

    Loader(String originalDataPath, String dbName, String tableName, int maxRowNum)
    {
        this.originalDataPath = originalDataPath;
        this.dbName = dbName;
        this.tableName = tableName;
        this.maxRowNum = maxRowNum;
    }

    boolean load() throws IOException, MetadataException
    {
        // init metadata service
        ConfigFactory configFactory = ConfigFactory.Instance();
        String metaHost = configFactory.getProperty("metadata.server.host");
        int metaPort = Integer.parseInt(configFactory.getProperty("metadata.server.port"));
        MetadataService metadataService = new MetadataService(metaHost, metaPort);
        // get columns of the specified table
        List<Column> columns = metadataService.getColumns(dbName, tableName);
        int colSize = columns.size();
        // record original column names and types
        String[] originalColNames = new String[colSize];
        String[] originalColTypes = new String[colSize];
        for (int i = 0; i < colSize; i++)
        {
            originalColNames[i] = columns.get(i).getName();
            originalColTypes[i] = columns.get(i).getType();
        }
        // get the latest layout for writing
        List<Layout> layouts = metadataService.getLayouts(dbName, tableName);
        Layout writingLayout = null;
        int writingLayoutVersion = -1;
        for (Layout layout : layouts)
        {
            if (layout.getPermission() > 0)
            {
                if (layout.getVersion() > writingLayoutVersion)
                {
                    writingLayout = layout;
                    writingLayoutVersion = layout.getVersion();
                }
            }
        }
        // no layouts for writing currently
        if (writingLayout == null)
        {
            return false;
        }
        // get the column order of the latest writing layout
        Order order = JSON.parseObject(writingLayout.getOrder(), Order.class);
        List<String> layoutColumnOrder = order.getColumnOrder();
        // get path of loading
        String loadingDataPath = writingLayout.getOrderPath();
        if (!loadingDataPath.endsWith("/"))
        {
            loadingDataPath += "/";
        }
        // check size consistency
        if (layoutColumnOrder.size() != colSize)
        {
            return false;
        }
        // map the column order of the latest writing layout to the original column order
        int[] orderMapping = new int[colSize];
        List<String> originalColNameList = Arrays.asList(originalColNames);
        for (int i = 0; i < colSize; i++)
        {
            int index=  originalColNameList.indexOf(layoutColumnOrder.get(i));
            if (index >= 0)
            {
                orderMapping[i] = index;
            }
            else
            {
                return false;
            }
        }
        // construct pixels schema based on the column order of the latest writing layout
        StringBuilder schemaBuilder = new StringBuilder("struct<");
        for (int i = 0; i < colSize; i++)
        {
            String name = layoutColumnOrder.get(i);
            String type = originalColTypes[orderMapping[i]];
            schemaBuilder.append(name).append(":").append(type)
                    .append(",");
        }
        schemaBuilder.replace(schemaBuilder.length() - 1, schemaBuilder.length(), ">");
        return executeLoad(originalDataPath, loadingDataPath, schemaBuilder.toString(), orderMapping, configFactory, maxRowNum);
    }

    protected abstract boolean executeLoad(String originalDataPath, String loadingDataPath, String schema,
                                           int[] orderMapping, ConfigFactory configFactory, int maxRowNum)
            throws IOException;
}
