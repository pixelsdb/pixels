package cn.edu.ruc.iir.pixels.test;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Compact;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Order;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.LayoutDao;
import com.alibaba.fastjson.JSON;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * pixels
 *
 * @author guodong
 */
public class UpdateCacheLayout
{
    @Test
    public void updateCacheLayout()
    {
        String cacheFile = "/Users/Jelly/Desktop";
        try {
            MetadataService metadataService = new MetadataService("dbiir01", 18888);
            Layout layoutv1 = metadataService.getLayout("pixels", "test_1187", 1).get(0);
            Order layoutOrder = layoutv1.getOrderObject();
            List<String> columnOrder = layoutOrder.getColumnOrder();

            BufferedReader reader = new BufferedReader(new FileReader(cacheFile));
            String line;
            List<Integer> orderIds = new ArrayList<>();
            while ((line = reader.readLine()) != null)
            {
                String colName = line.trim();
                int id = columnOrder.indexOf(colName);
                orderIds.add(id);
            }
            reader.close();

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
            Layout layoutv2 = new Layout();
            layoutv2.setId(-1);
            layoutv2.setPermission(1);
            layoutv2.setVersion(2);
            layoutv2.setCreateAt(System.currentTimeMillis());
            layoutv2.setOrder(layoutv1.getOrder());
            layoutv2.setOrderPath(layoutv1.getOrderPath());
            layoutv2.setCompact(JSON.toJSONString(compactv2));
            layoutv2.setCompactPath(layoutv1.getCompactPath());
            layoutv2.setSplits(layoutv1.getSplits());
            layoutv2.setTable(layoutv1.getTable());
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
        Layout layout = metadataService.getLayout("pixels", "test_1187", 3).get(0);
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
        Layout layout = metadataService.getLayout("pixels", "test_1187", 3).get(0);
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
