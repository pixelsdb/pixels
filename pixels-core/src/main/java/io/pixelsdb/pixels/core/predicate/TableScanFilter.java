/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.core.predicate;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.util.Map;
import java.util.SortedMap;

/**
 * The filters used in table scan.
 *
 * Created at: 08/04/2022
 * Author: hank
 */
@JSONType(includes = {"schemaName", "tableName", "columnFilters"})
public class TableScanFilter
{
    @JSONField(name = "schemaName", ordinal = 0)
    private final String schemaName;
    @JSONField(name = "tableName", ordinal = 1)
    private final String tableName;
    /**
     * Mapping from column id (in the scan result) to column filter.
     */
    @JSONField(name = "columnFilters", ordinal = 2)
    private final SortedMap<Integer, ColumnFilter> columnFilters;

    @JSONCreator
    public TableScanFilter(String schemaName, String tableName, SortedMap<Integer, ColumnFilter> columnFilters)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnFilters = columnFilters;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public SortedMap<Integer, ColumnFilter> getColumnFilters()
    {
        return columnFilters;
    }

    public ColumnFilter getColumnFilter(int columnId)
    {
        return columnFilters.get(columnId);
    }

    /**
     * Filter all the rows in the row batch using this table scan filter.
     * In the returned BitSet, the ith bit is set if the ith row in the row batch matches the filter.
     *
     * The result bitset can be reused.
     *
     * @param rowBatch the row batch.
     * @param result the filter result.
     * @param tmp the temporary bitmap to be used in filter.
     */
    public void doFilter(VectorizedRowBatch rowBatch, Bitmap result, Bitmap tmp)
    {
        // set all bits to true.
        result.set(0, rowBatch.size);
        for (Map.Entry<Integer, ColumnFilter> entry : this.columnFilters.entrySet())
        {
            int columnId = entry.getKey();
            ColumnFilter columnFilter = entry.getValue();
            columnFilter.doFilter(rowBatch.cols[columnId], 0, rowBatch.size, tmp);
            result.and(tmp);
        }
    }
}
