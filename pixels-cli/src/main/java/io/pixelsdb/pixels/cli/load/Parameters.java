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
package io.pixelsdb.pixels.cli.load;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Order;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

import static io.pixelsdb.pixels.cli.Main.validateOrderOrCompactPath;

public class Parameters
{
    private final String dbName;
    private final String tableName;
    private final int maxRowNum;
    private final String regex;
    private String loadingPath;
    private String schema;
    private int[] orderMapping;
    private final boolean enableEncoding;

    public String getLoadingPath()
    {
        return loadingPath;
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

    public boolean isEnableEncoding()
    {
        return enableEncoding;
    }

    public Parameters(String dbName, String tableName, int maxRowNum, String regex,
                      boolean enableEncoding, @Nullable String loadingPath)
    {
        this.dbName = dbName;
        this.tableName = tableName;
        this.maxRowNum = maxRowNum;
        this.regex = regex;
        this.loadingPath = loadingPath;
        this.enableEncoding = enableEncoding;
    }

    /**
     * Initialize the extra parameters, including schema and order mapping.
     * If loading path is not set in the constructor, this method will set it to the ordered path in metadata.
     * @param configFactory the config factory of pixels.
     * @return true if successful.
     * @throws MetadataException
     * @throws InterruptedException
     */
    public boolean initExtra(ConfigFactory configFactory) throws MetadataException, InterruptedException
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
        if(this.loadingPath == null)
        {
            this.loadingPath = writingLayout.getOrderPath();
            validateOrderOrCompactPath(this.loadingPath);
        }
        // init the params
        this.schema = schemaBuilder.toString();
        this.orderMapping = orderMapping;
        metadataService.shutdown();
        return true;
    }
}
