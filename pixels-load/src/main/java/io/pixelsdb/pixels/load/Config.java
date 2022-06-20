/*
 * Copyright 2018-2019 PixelsDB.
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
package io.pixelsdb.pixels.load;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Order;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.Arrays;
import java.util.List;

public class Config
{
    private String dbName;
    private String tableName;
    private int maxRowNum;
    private String regex;

    private String format;

    private String pixelsPath;
    private String schema;
    private int[] orderMapping;

    public String getPixelsPath()
    {
        return pixelsPath;
    }

    public String getSchema()
    {
        return schema;
    }

    public int[] getOrderMapping()
    {
        return orderMapping;
    }

    public int getMaxRowNum()
    {
        return maxRowNum;
    }

    public String getRegex()
    {
        return regex;
    }

    public String getFormat()
    {
        return format;
    }

    public Config(String dbName, String tableName, int maxRowNum, String regex, String format, String pixelsPath)
    {
        this.dbName = dbName;
        this.tableName = tableName;
        this.maxRowNum = maxRowNum;
        this.regex = regex;
        this.format = format;
        this.pixelsPath = pixelsPath;
    }

    public boolean load(ConfigFactory configFactory) throws MetadataException, InterruptedException
    {
        // init metadata service
        String metaHost = configFactory.getProperty("metadata.server.host");
        int metaPort = Integer.parseInt(configFactory.getProperty("metadata.server.port"));
        MetadataService metadataService = new MetadataService(metaHost, metaPort);
        // get columns of the specified table
        List<Column> columns = metadataService.getColumns(dbName, tableName, false);
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
            if (layout.isWritable())
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
            int index = originalColNameList.indexOf(layoutColumnOrder.get(i));
            if (index >= 0)
            {
                orderMapping[i] = index;
            } else
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
            /**
             * Issue #100:
             * Refer TypeDescription, ColumnReader, and ColumnWriter for how Pixels
             * deals with data types.
             */
            schemaBuilder.append(name).append(":").append(type)
                    .append(",");
        }
        schemaBuilder.replace(schemaBuilder.length() - 1, schemaBuilder.length(), ">");

        // get path of loading
        if(this.pixelsPath == null)
        {
            String loadingDataPath = writingLayout.getOrderPath();
            if (!loadingDataPath.endsWith("/"))
            {
                loadingDataPath += "/";
            }
            this.pixelsPath = loadingDataPath;
        }
        else
        {
            if (!this.pixelsPath.endsWith("/"))
            {
                this.pixelsPath += "/";
            }
        }
        // init the params
        this.schema = schemaBuilder.toString();
        this.orderMapping = orderMapping;
        metadataService.shutdown();
        return true;
    }

}
