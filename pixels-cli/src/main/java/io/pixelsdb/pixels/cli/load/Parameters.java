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

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Ordered;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;

import java.util.Arrays;
import java.util.List;

import static io.pixelsdb.pixels.cli.Main.validateOrderedOrCompactPaths;

public class Parameters
{
    private final String dbName;
    private final String tableName;
    private final int maxRowNum;
    private final String regex;
    private List<Path> loadingPaths;
    private String schema;
    private int[] orderMapping;
    private final EncodingLevel encodingLevel;
    private final boolean nullsPadding;
    private final MetadataService metadataService;

    public List<Path> getLoadingPaths()
    {
        return loadingPaths;
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

    public EncodingLevel getEncodingLevel()
    {
        return encodingLevel;
    }

    public boolean isNullsPadding()
    {
        return nullsPadding;
    }

    public Parameters(String dbName, String tableName, int maxRowNum, String regex,
                      EncodingLevel encodingLevel, boolean nullsPadding, MetadataService metadataService)
    {
        this.dbName = dbName;
        this.tableName = tableName;
        this.maxRowNum = maxRowNum;
        this.regex = regex;
        this.loadingPaths = null;
        this.encodingLevel = encodingLevel;
        this.nullsPadding = nullsPadding;
        this.metadataService = metadataService;
    }

    /**
     * Initialize the extra parameters, including schema and order mapping.
     * If loading path is not set in the constructor, this method will set it to the ordered path in metadata.
     * @return true if successful.
     * @throws MetadataException
     * @throws InterruptedException
     */
    public boolean initExtra() throws MetadataException, InterruptedException
    {
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
        Layout writableLayout = metadataService.getWritableLayout(dbName, tableName);
        // no layouts for writing currently
        if (writableLayout == null)
        {
            return false;
        }
        // get the column order of the latest writing layout
        Ordered ordered = writableLayout.getOrdered();
        List<String> layoutColumnOrder = ordered.getColumnOrder();
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
            if(name.equalsIgnoreCase("pixels_commit_timestamp"))
            {
                return false;
            }
            /**
             * Issue #100:
             * Refer TypeDescription, ColumnReader, and ColumnWriter for how Pixels
             * deals with data types.
             */
            schemaBuilder.append(name).append(":").append(type)
                    .append(",");
        }
        // add timestamp column
        schemaBuilder.append("pixels_commit_timestamp:timestamp>");

        // get path of loading
        if(this.loadingPaths == null)
        {
            this.loadingPaths = writableLayout.getOrderedPaths();
            validateOrderedOrCompactPaths(this.loadingPaths);
        }
        // init the params
        this.schema = schemaBuilder.toString();
        this.orderMapping = orderMapping;
        // Issue #658: do not shut down metadata service here.
        return true;
    }
}
