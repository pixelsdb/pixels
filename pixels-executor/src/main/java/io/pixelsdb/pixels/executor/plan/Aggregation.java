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
     * The information of the output endpoint of the aggregation result.
     */
    private final OutputEndPoint outputEndPoint;
    /**
     * The origin table that the aggregation is performed.
     */
    private final Table originTable;

    public Aggregation(String[] groupKeyColumnAlias, String[] resultColumnAlias,
                       int[] groupKeyColumnIds, int[] aggregateColumnIds,
                       FunctionType[] functionTypes, OutputEndPoint outputEndPoint,
                       Table originTable)
    {
        this.groupKeyColumnAlias = groupKeyColumnAlias;
        this.resultColumnAlias = resultColumnAlias;
        this.groupKeyColumnIds = groupKeyColumnIds;
        this.aggregateColumnIds = aggregateColumnIds;
        this.functionTypes = functionTypes;
        this.outputEndPoint = outputEndPoint;
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

    public OutputEndPoint getOutputEndPoint()
    {
        return outputEndPoint;
    }

    public Table getOriginTable()
    {
        return originTable;
    }
}
