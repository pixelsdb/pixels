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
package io.pixelsdb.pixels.planner.plan.logical;

import io.pixelsdb.pixels.executor.aggregation.FunctionType;

/**
 * {@link Aggregation} is the aggregation information on the origin table.
 * @author hank
 * @date 05/07/2022
 */
public class Aggregation
{
    /**
     * The alias of the grouping key columns in the aggregation result.
     */
    private final String[] groupKeyColumnAlias;
    /**
     * The alias of the synthetic aggregation columns in the aggregation result.
     */
    private final String[] resultColumnAlias;
    /**
     * The display name of the synthetic aggregation columns in the aggregation result.
     * They should be parsed by the TypeDescription in Pixels.
     */
    private final String[] resultColumnTypes;
    /**
     * If a group key column appears in the aggregation output,
     * the corresponding element in this array would be true, and vice versa.
     */
    private final boolean[] groupKeyColumnProjection;
    /**
     * The index of the grouping key columns in the origin table.
     */
    private final int[] groupKeyColumnIds;
    /**
     * The index of group aggregated columns in the origin table.
     */
    private final int[] aggregateColumnIds;
    /**
     * The aggregate functions that are applied on the aggregated columns.
     * TODO: currently, we only support one function per aggregated column.
     */
    private final FunctionType[] functionTypes;
    /**
     * The origin table that the aggregation is performed.
     */
    private final Table originTable;

    public Aggregation(String[] groupKeyColumnAlias, String[] resultColumnAlias,
                       String[] resultColumnTypes, boolean[] groupKeyColumnProjection,
                       int[] groupKeyColumnIds, int[] aggregateColumnIds,
                       FunctionType[] functionTypes, Table originTable)
    {
        this.groupKeyColumnAlias = groupKeyColumnAlias;
        this.resultColumnAlias = resultColumnAlias;
        this.resultColumnTypes = resultColumnTypes;
        this.groupKeyColumnProjection = groupKeyColumnProjection;
        this.groupKeyColumnIds = groupKeyColumnIds;
        this.aggregateColumnIds = aggregateColumnIds;
        this.functionTypes = functionTypes;
        this.originTable = originTable;
    }

    public String[] getGroupKeyColumnAlias()
    {
        return groupKeyColumnAlias;
    }

    public String[] getResultColumnAlias()
    {
        return resultColumnAlias;
    }

    public String[] getResultColumnTypes()
    {
        return resultColumnTypes;
    }

    public boolean[] getGroupKeyColumnProjection()
    {
        return groupKeyColumnProjection;
    }

    public int[] getGroupKeyColumnIds()
    {
        return groupKeyColumnIds;
    }

    public int[] getAggregateColumnIds()
    {
        return aggregateColumnIds;
    }

    public FunctionType[] getFunctionTypes()
    {
        return functionTypes;
    }

    public Table getOriginTable()
    {
        return originTable;
    }
}
