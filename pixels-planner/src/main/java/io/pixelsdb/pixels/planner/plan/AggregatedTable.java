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
package io.pixelsdb.pixels.planner.plan;

import com.google.common.base.Objects;
import com.google.common.collect.ObjectArrays;

/**
 * @author hank
 * @date 05/07/2022
 */
public class AggregatedTable implements Table
{
    private final String schemaName;
    private final String tableName;
    private final String tableAlias;
    private final String[] columnNames;
    private final Aggregation aggregation;

    /**
     * The {@link AggregatedTable#columnNames} of this class is constructed by the colum alias
     * of the origin table on which the aggregation is computed and the column alias of the
     * aggregation result columns that are computed by the aggregation functions.
     *
     * @param schemaName the schema name
     * @param tableName the table name
     * @param tableAlias the table alias
     * @param aggregation the information of the aggregation
     */
    public AggregatedTable(String schemaName, String tableName, String tableAlias, Aggregation aggregation)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableAlias = tableAlias;
        this.aggregation = aggregation;
        this.columnNames =  ObjectArrays.concat(
                aggregation.getGroupKeyColumnAlias(), aggregation.getResultColumnAlias(), String.class);
    }

    @Override
    public TableType getTableType()
    {
        return TableType.AGGREGATED;
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
    public String[] getColumnNames()
    {
        return columnNames;
    }

    public Aggregation getAggregation()
    {
        return aggregation;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregatedTable that = (AggregatedTable) o;
        return Objects.equal(schemaName, that.schemaName) &&
                Objects.equal(tableName, that.tableName) &&
                Objects.equal(tableAlias, that.tableAlias) &&
                Objects.equal(columnNames, that.columnNames) &&
                Objects.equal(aggregation, that.aggregation);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schemaName, tableName, tableAlias, columnNames, aggregation);
    }
}
