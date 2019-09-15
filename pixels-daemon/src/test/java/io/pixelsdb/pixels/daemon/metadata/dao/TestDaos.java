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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.daemon.metadata.dao;

import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Order;
import org.junit.Test;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestDaos
{
    @Test
    public void testSchema ()
    {
        SchemaDao schemaDao = new SchemaDao();
        MetadataProto.Schema schema = schemaDao.getByName("pixels");
        System.out.println(schema.getId() + ", " + schema.getName() + ", " + schema.getDesc());
    }

    @Test
    public void testTable ()
    {
        TableDao tableDao = new TableDao();
        List<MetadataProto.Table> tables = tableDao.getByName("test_105");
        for (MetadataProto.Table table : tables)
        {
            System.out.println(table.getId() + ", " + table.getSchemaId());
        }
    }

    @Test
    public void testLayout ()
    {
        String schemaName = "pixels";
        String tableName = "test_1187";

        SchemaDao schemaDao = new SchemaDao();
        TableDao tableDao = new TableDao();
        ColumnDao columnDao = new ColumnDao();
        LayoutDao layoutDao = new LayoutDao();

        MetadataProto.Schema schema = schemaDao.getByName(schemaName);
        MetadataProto.Table table = tableDao.getByNameAndSchema(tableName, schema);
        List<MetadataProto.Column> columns = columnDao.getByTable(table);
        List<MetadataProto.Layout> layouts = layoutDao.getAllByTable(table);

        for (MetadataProto.Column column : columns)
        {
            System.out.println(column.getName() + ", " + column.getType());
        }

        for (MetadataProto.Layout layout : layouts)
        {
            System.out.println(layout.getOrderPath());
        }
    }

    @Test
    public void checkCompactLayout ()
    {
        String schemaName = "pixels";
        String tableName = "test_1187";

        SchemaDao schemaDao = new SchemaDao();
        TableDao tableDao = new TableDao();
        ColumnDao columnDao = new ColumnDao();
        LayoutDao layoutDao = new LayoutDao();

        MetadataProto.Schema schema = schemaDao.getByName(schemaName);
        MetadataProto.Table table = tableDao.getByNameAndSchema(tableName, schema);
        columnDao.getByTable(table);
        List<MetadataProto.Layout> layouts = layoutDao.getAllByTable(table);


        MetadataProto.Layout layout = null;

        for (MetadataProto.Layout layout1 : layouts)
        {
            if (layout1.getId() == 14)
            {
                layout = layout1;
                break;
            }
        }

        Layout layout1 = new Layout(layout);
        List<String> columnOrder = layout1.getOrderObject().getColumnOrder();
        int cacheBorder = layout1.getCompactObject().getCacheBorder();
        List<String> columnletOrder = layout1.getCompactObject().getColumnletOrder();
        Set<String> cachedColumns = new HashSet<>();
        for (int i = 0; i < cacheBorder; ++i)
        {
            int columnId = Integer.parseInt(columnletOrder.get(i).split(":")[1]);
            cachedColumns.add(columnOrder.get(columnId));
        }

        for (String column : cachedColumns)
        {
            System.out.println(column);
        }
    }

    // get from dbiir27
    @Test
    public void getLayout()
            throws IOException
    {
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File("/Users/Jelly/Desktop/dbiir10-splits")));
        LayoutDao layoutDao = new LayoutDao();
        Layout layout = new Layout(layoutDao.getById(21));
        Order order = layout.getOrderObject();
        List<String> columnOrder = order.getColumnOrder();
        for (String col : columnOrder)
        {
            writer.write(col);
            writer.newLine();
        }
        writer.close();
    }

    // update dbiir10
    @Test
    public void updateLayout()
            throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader(new File("/Users/Jelly/Desktop/splits")));
        String splits = reader.readLine();
        LayoutDao layoutDao = new LayoutDao();
        layoutDao.update(layoutDao.getById(10).toBuilder().setSplits(splits).build());
        reader.close();
    }
}
