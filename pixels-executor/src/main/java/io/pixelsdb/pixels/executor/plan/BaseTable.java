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

import com.google.common.base.Objects;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;

/**
 * The table that is used in joins.
 * @author hank
 * @date 26/05/2022
 */
public class BaseTable implements Table
{
    private final String schemaName;
    private final String tableName;
    private final String tableAlias;
    private final int[] keyColumnIds;
    private final String[] columnNames;
    private final TableScanFilter filter;

    public BaseTable(String schemaName, String tableName, String tablAlias,
                     int[] keyColumnIds, String[] columnNames, TableScanFilter filter)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableAlias = tablAlias;
        this.keyColumnIds = keyColumnIds;
        this.columnNames = columnNames;
        this.filter = filter;
    }

    @Override
    public boolean isBase()
    {
        return true;
    }

    @Override
    public String getSchemaName()
    {
        return schemaName;
    }

    @Override
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public String getTableAlias()
    {
        return tableAlias;
    }

    @Override
    public int[] getKeyColumnIds()
    {
        return keyColumnIds;
    }

    @Override
    public String[] getColumnNames()
    {
        return columnNames;
    }

    public TableScanFilter getFilter()
    {
        return filter;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseTable table = (BaseTable) o;
        return Objects.equal(schemaName, table.schemaName) &&
                Objects.equal(tableName, table.tableName) &&
                Objects.equal(tableAlias, table.tableAlias) &&
                Objects.equal(keyColumnIds, table.keyColumnIds) &&
                Objects.equal(columnNames, table.columnNames) &&
                Objects.equal(filter, table.filter);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schemaName, tableName, tableAlias,
                keyColumnIds, columnNames, filter);
    }
}
