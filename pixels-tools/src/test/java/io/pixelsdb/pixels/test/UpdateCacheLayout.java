/*
 * Copyright 2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.test;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Compact;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Order;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.DaoFactory;
import io.pixelsdb.pixels.daemon.metadata.dao.LayoutDao;
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

        try
        {
            MetadataService metadataService = new MetadataService(metaHost, metaPort);
            Layout layoutv1 = metadataService.getLayout(schemaName, tableName, 1);

            Order layoutOrder = layoutv1.getOrderObject();
            List<String> columnOrder = layoutOrder.getColumnOrder();
            List<Column> columns = metadataService.getColumns(schemaName, tableName, true);
            Map<String, Double> columnSizeMap = new HashMap<>();
            for (Column column : columns)
            {
                columnSizeMap.put(column.getName(), column.getChunkSize());
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

            LayoutDao layoutDao = DaoFactory.Instance().getLayoutDao("rdb");
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
            metadataService.shutdown();
        }
        catch (MetadataException | IOException | InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void getLayoutOrder()
            throws MetadataException, IOException, InterruptedException
    {
        MetadataService metadataService = new MetadataService("dbiir01", 18888);
        Layout layout = metadataService.getLayout("pixels", "test_1187", 3);
        metadataService.shutdown();
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
            throws MetadataException, InterruptedException
    {
        MetadataService metadataService = new MetadataService("dbiir01", 18888);
        Layout layout = metadataService.getLayout("pixels", "test_1187", 3);
        metadataService.shutdown();
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
