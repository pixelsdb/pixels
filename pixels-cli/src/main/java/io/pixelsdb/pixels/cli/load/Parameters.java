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
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

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
    private int[] pkMapping;
    private TypeDescription pkTypeDescription;
    private final EncodingLevel encodingLevel;
    private final boolean nullsPadding;
    private final MetadataService metadataService;
    private final long transId;
    private final long timestamp;
    private SinglePointIndex index;

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

    public long getTransId() { return transId; }

    public long getTimestamp() { return timestamp; }

    public int[] getPkMapping()
    {
        return pkMapping;
    }

    public SinglePointIndex getIndex()
    {
        return index;
    }

    public MetadataService getMetadataService()
    {
        return metadataService;
    }

    public TypeDescription getPkTypeDescription()
    {
        return pkTypeDescription;
    }

    public Parameters(String dbName, String tableName, int maxRowNum, String regex, EncodingLevel encodingLevel,
                      boolean nullsPadding, MetadataService metadataService, long transId, long timestamp)
    {
        this.dbName = dbName;
        this.tableName = tableName;
        this.maxRowNum = maxRowNum;
        this.regex = regex;
        this.loadingPaths = null;
        this.encodingLevel = encodingLevel;
        this.nullsPadding = nullsPadding;
        this.metadataService = metadataService;
        this.transId = transId;
        this.timestamp = timestamp;
        this.index = null;
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
        // get table info
        Table table = metadataService.getTable(dbName, tableName);

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
        if(this.loadingPaths == null)
        {
            this.loadingPaths = writableLayout.getOrderedPaths();
            validateOrderedOrCompactPaths(this.loadingPaths);
        }

        // get index information
        // TODO: Support Secondary Index
        try
        {
            this.index = metadataService.getPrimaryIndex(table.getId());
        } catch (MetadataException ignored)
        {
            // Primary key not required
        }

        if(index != null)
        {
            int[] orderKeyColIds = new int[index.getKeyColumns().getKeyColumnIds().size()];
            List<String> orderKeyColNames = new LinkedList<>();
            List<String> orderKeyColTypes = new LinkedList<>();
            if (!index.isUnique()) {
                throw new MetadataException("Non Unique Index is not supported, Schema:" + dbName + " Table: " + tableName);
            }

            int keyColumnIdx = 0;
            for (Integer keyColumnId : index.getKeyColumns().getKeyColumnIds()) {
                int i = IntStream.range(0, columns.size())
                        .filter(idx -> columns.get(idx).getId() == keyColumnId)
                        .findFirst()
                        .orElse(-1);
                if(i == -1)
                {
                    throw new MetadataException("Cant find key column id: " + keyColumnId + " in table "
                            + table.getName() + " schema id is " + table.getSchemaId());
                }
                orderKeyColIds[keyColumnIdx++] = i;
                orderKeyColNames.add(columns.get(i).getName());
                orderKeyColTypes.add(columns.get(i).getType());
            }
            this.pkTypeDescription = TypeDescription.createSchemaFromStrings(orderKeyColNames, orderKeyColTypes);
            this.pkMapping = orderKeyColIds;
        }

        // init the params
        this.schema = schemaBuilder.toString();
        this.orderMapping = orderMapping;
        // Issue #658: do not shut down metadata service here.
        return true;
    }
}
