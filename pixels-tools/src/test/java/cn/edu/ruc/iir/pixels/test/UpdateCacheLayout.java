package cn.edu.ruc.iir.pixels.test;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.*;
import cn.edu.ruc.iir.pixels.daemon.MetadataProto;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.LayoutDao;
import com.alibaba.fastjson.JSON;
import org.junit.Test;

import java.io.*;
import java.util.*;

/**
 * pixels
 *
 * @author guodong
 */
public class UpdateCacheLayout
{
    public static void main(String[] args)
    {
        UpdateCacheLayout object = new UpdateCacheLayout();
        object.updateCacheLayout();
    }

    private void updateCacheLayout()
    {
        final String cacheFile = "/Users/Jelly/Desktop/pixels/cache/Apr15/updated_cached_cols";
        final String metaHost = "dbiir27";
        final int metaPort = 18888;
        final String schemaName = "pixels";
        final String tableName = "test_1187";

        try {
            MetadataService metadataService = new MetadataService(metaHost, metaPort);
            Layout layoutv1 = metadataService.getLayout(schemaName, tableName, 1);

            Order layoutOrder = layoutv1.getOrderObject();
            List<String> columnOrder = layoutOrder.getColumnOrder();
            List<Column> columns = metadataService.getColumns(schemaName, tableName);
            Map<String, Double> columnSizeMap = new HashMap<>();
            for (Column column : columns)
            {
                columnSizeMap.put(column.getName(), column.getSize());
            }

            BufferedReader reader = new BufferedReader(new FileReader(cacheFile));
            String line;
            Set<Integer> orderIds = new HashSet<>();
            double size = 0d;
            while ((line = reader.readLine()) != null)
            {
                String[] colNames = line.trim().split(",");
                for (String colName : colNames)
                {
                    colName = colName.trim().toLowerCase();
                    int id = columnOrder.indexOf(colName);
                    orderIds.add(id);
                    size += columnSizeMap.getOrDefault(colName, 0.0d);
                }
            }
            reader.close();

            System.out.println("Estimated size of caching: " + (32 * size * 26 / 1024.0 / 1024.0) + " MB");
            StringBuilder sb = new StringBuilder();
            for (int id : orderIds)
            {
                sb.append(columnOrder.get(id)).append(",");
            }
            System.out.println(sb.toString());

            Compact compactv2 = layoutv1.getCompactObject();
            compactv2.setCacheBorder(32 * orderIds.size());
            compactv2.setNumRowGroupInBlock(32);
            compactv2.setNumColumn(orderIds.size());
            compactv2.setColumnletOrder(new ArrayList<>());

            for (int orderId : orderIds)
            {
                for (int i = 0; i < 32; i++)
                {
                    String columnlet = "" + i + ":" + orderId;
                    compactv2.addColumnletOrder(columnlet);
                }
            }

            LayoutDao layoutDao = new LayoutDao();
            MetadataProto.Layout layoutv2 = MetadataProto.Layout.newBuilder()
            .setId(21)
            .setPermission(MetadataProto.Layout.Permission.READ_WRITE)
            .setVersion(2)
            .setCreateAt(System.currentTimeMillis())
            .setOrder(layoutv1.getOrder())
            .setOrderPath(layoutv1.getOrderPath())
            .setCompact(JSON.toJSONString(compactv2))
            .setCompactPath(layoutv1.getCompactPath())
            .setSplits(layoutv1.getSplits())
            .setTableId(layoutv1.getTableId()).build();
            layoutDao.save(layoutv2);
        }
        catch (MetadataException | IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getLayoutOrder()
            throws MetadataException, IOException
    {
        MetadataService metadataService = new MetadataService("dbiir01", 18888);
        Layout layout = metadataService.getLayout("pixels", "test_1187", 3);
        Order order = layout.getOrderObject();
        List<String> orderCols = order.getColumnOrder();
        BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/Jelly/Desktop/pixels/cache/layout_order_dbiir01_v3"));
        for (String col : orderCols)
        {
            writer.write(col);
            writer.newLine();
        }
        writer.close();
    }

    @Test
    public void getMetadata()
            throws MetadataException
    {
        MetadataService metadataService = new MetadataService("dbiir01", 18888);
        Layout layout = metadataService.getLayout("pixels", "test_1187", 3);
        Compact compact = layout.getCompactObject();
        List<String> cachedColumnlets = compact.getColumnletOrder().subList(0, compact.getCacheBorder());
        List<String> columnOrder = layout.getOrderObject().getColumnOrder();
        Set<String> colSet = new HashSet<>();
        for (String columnlet : cachedColumnlets)
        {
            int rgId = Integer.parseInt(columnlet.split(":")[0]);
            int colId = Integer.parseInt(columnlet.split(":")[1]);
            System.out.println("" + rgId + ":" + columnOrder.get(colId));
            colSet.add(columnOrder.get(colId));
        }
        for (String col : colSet)
        {
            System.out.println("Cached column: " + col);
        }
    }
}
