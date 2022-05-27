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
package io.pixelsdb.pixels.executor.plan;

import io.pixelsdb.pixels.executor.predicate.TableScanFilter;

/**
 * The table that is used in joins.
 * @author hank
 * @date 26/05/2022
 */
public class Table
{
    /**
     * Whether this is a join pipeline of base tables.
     */
    private final boolean isBase;
    private final String schemaName;
    private final String tableName;
    private final int[] keyColumnIds;
    private final String[] includeCols;
    private final TableScanFilter filter;

    public Table(boolean isBase, String schemaName, String tableName, int[] keyColumnIds,
                 String[] includeCols, TableScanFilter filter)
    {
        this.isBase = isBase;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.keyColumnIds = keyColumnIds;
        this.includeCols = includeCols;
        this.filter = filter;
    }

    public boolean isBase()
    {
        return isBase;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String[] getIncludeCols()
    {
        return includeCols;
    }

    public TableScanFilter getFilter()
    {
        return filter;
    }
}
