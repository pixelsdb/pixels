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
package io.pixelsdb.pixels.executor.lambda.domain;

import io.pixelsdb.pixels.executor.aggregation.FunctionType;

/**
 * @author hank
 * @date 05/07/2022
 */
public class AggregationInfo
{
    /**
     * The column alias of the group key columns in the aggregation result.
     */
    private String[] groupColumnAlias;
    /**
     * The column alias of the aggregated columns in the aggregation result.
     */
    private String[] resultColumnAlias;
    /**
     * The column ids of the group key columns in the origin table.
     */
    private int[] groupKeyColumnIds;
    /**
     * The column ids of the aggregate columns in the origin table.
     */
    private int[] aggregateColumnIds;
    /**
     * The aggregation functions, in the same order of resultColumnAlias.
     */
    private FunctionType[] functionTypes;

    /**
     * Default constructor for Jackson.
     */
    public AggregationInfo() { }

    public AggregationInfo(String[] groupColumnAlias, String[] resultColumnAlias, int[] groupKeyColumnIds, int[] aggregateColumnIds, FunctionType[] functionTypes)
    {
        this.groupColumnAlias = groupColumnAlias;
        this.resultColumnAlias = resultColumnAlias;
        this.groupKeyColumnIds = groupKeyColumnIds;
        this.aggregateColumnIds = aggregateColumnIds;
        this.functionTypes = functionTypes;
    }

    public String[] getGroupColumnAlias()
    {
        return groupColumnAlias;
    }

    public void setGroupColumnAlias(String[] groupColumnAlias)
    {
        this.groupColumnAlias = groupColumnAlias;
    }

    public String[] getResultColumnAlias()
    {
        return resultColumnAlias;
    }

    public void setResultColumnAlias(String[] resultColumnAlias)
    {
        this.resultColumnAlias = resultColumnAlias;
    }

    public int[] getGroupKeyColumnIds()
    {
        return groupKeyColumnIds;
    }

    public void setGroupKeyColumnIds(int[] groupKeyColumnIds)
    {
        this.groupKeyColumnIds = groupKeyColumnIds;
    }

    public int[] getAggregateColumnIds()
    {
        return aggregateColumnIds;
    }

    public void setAggregateColumnIds(int[] aggregateColumnIds)
    {
        this.aggregateColumnIds = aggregateColumnIds;
    }

    public FunctionType[] getFunctionTypes()
    {
        return functionTypes;
    }

    public void setFunctionTypes(FunctionType[] functionTypes)
    {
        this.functionTypes = functionTypes;
    }
}
