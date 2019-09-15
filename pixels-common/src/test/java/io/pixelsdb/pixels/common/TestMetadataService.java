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
package io.pixelsdb.pixels.common;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestMetadataService
{
    MetadataService instance = null;

    @Before
    public void init()
    {
        this.instance = new MetadataService("dbiir27", 18888);
    }

    @After
    public void shutdown() throws InterruptedException
    {
        this.instance.shutdown();
    }

    @Test
    public void testGetSchemaNames() throws InterruptedException, MetadataException
    {
        for (int i = 1; i <= 5; i++)
        {
            Thread t = new Thread(() -> {
                List<Schema> schemas = null;
                try
                {
                    schemas = instance.getSchemas();
                }
                catch (MetadataException e)
                {
                    e.printStackTrace();
                }
                System.out.println("Thread: " + schemas.size());
            });
            t.start();
            t.join();
        }

        List<Schema> schemas = instance.getSchemas();
        System.out.println("Command: " + schemas.size());
    }

    @Test
    public void testGetTableNames() throws MetadataException
    {
        String schemaName = "pixels";
        List<String> tableList = new ArrayList<String>();
        List<Table> tables = instance.getTables(schemaName);
        for (Table t : tables)
        {
            tableList.add(t.getName());
        }
        System.out.println("Show tables, " + tableList.toString());
    }

    @Test
    public void testGetColumnsBySchemaNameAndTblName() throws MetadataException
    {
        List<Column> columns = instance.getColumns("pixels", "test_105");
        for (Column column : columns)
        {
            System.out.println(column.getName() + ", " + column.getType());
        }
    }

    @Test
    public void testGetTableLayouts() throws MetadataException
    {
        long start = System.currentTimeMillis();
        List<Layout> layouts = instance.getLayouts("pixels", "test_105");
        long end = System.currentTimeMillis();
        System.out.println("Last: " + (end - start));
        System.out.println(layouts.get(0).getSplits());

        for (Layout layout : layouts)
        {
            // get index
            int version = layout.getVersion();
            Order order = JSON.parseObject(layout.getOrder(), Order.class);
            Splits splits = JSON.parseObject(layout.getSplits(), Splits.class);
            System.out.println(JSON.toJSONString(order));
            System.out.println(JSON.toJSONString(splits));
        }
    }

    @Test
    public void testGetTableLayoutsByVersion() throws MetadataException
    {
        long start = System.currentTimeMillis();
        Layout layout = instance.getLayout("pixels", "test_105", 1);
        long end = System.currentTimeMillis();
        System.out.println("Last: " + (end - start));
        System.out.println(layout.getSplits());
    }
}
