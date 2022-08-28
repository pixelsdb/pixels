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
public class PartialAggregationInfo
{
    /**
     * The column alias of the group-key columns in the aggregation result.
     */
    private String[] groupKeyColumnAlias;
    /**
     * The column alias of the aggregated columns in the aggregation result.
     */
    private String[] resultColumnAlias;
    /**
     * The display name of the data types of the result columns.
     * They should be parsed by the TypeDescription in Pixels.
     */
    private String[] resultColumnTypes;
    /**
     * The column ids of the group-key columns in the origin table.
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
     * Whether the partial aggregation result should be partitioned.
     */
    private boolean partition;
    /**
     * The number of partitions for the aggregations result.
     */
    private int numPartition;

    /**
     * Default constructor for Jackson.
     */
    public PartialAggregationInfo() { }

    public PartialAggregationInfo(String[] groupKeyColumnAlias, String[] resultColumnAlias,
                                  String[] resultColumnTypes, int[] groupKeyColumnIds,
                                  int[] aggregateColumnIds, FunctionType[] functionTypes,
                                  boolean partition, int numPartition)
    {
        this.groupKeyColumnAlias = groupKeyColumnAlias;
        this.resultColumnAlias = resultColumnAlias;
        this.resultColumnTypes = resultColumnTypes;
        this.groupKeyColumnIds = groupKeyColumnIds;
        this.aggregateColumnIds = aggregateColumnIds;
        this.functionTypes = functionTypes;
        this.partition = partition;
        this.numPartition = numPartition;
    }

    public String[] getGroupKeyColumnAlias()
    {
        return groupKeyColumnAlias;
    }

    public void setGroupKeyColumnAlias(String[] groupKeyColumnAlias)
    {
        this.groupKeyColumnAlias = groupKeyColumnAlias;
    }

    public String[] getResultColumnAlias()
    {
        return resultColumnAlias;
    }

    public void setResultColumnAlias(String[] resultColumnAlias)
    {
        this.resultColumnAlias = resultColumnAlias;
    }

    public String[] getResultColumnTypes()
    {
        return resultColumnTypes;
    }

    public void setResultColumnTypes(String[] resultColumnTypes)
    {
        this.resultColumnTypes = resultColumnTypes;
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

    public boolean isPartition()
    {
        return partition;
    }

    public void setPartition(boolean partition)
    {
        this.partition = partition;
    }

    public int getNumPartition()
    {
        return numPartition;
    }

    public void setNumPartition(int numPartition)
    {
        this.numPartition = numPartition;
    }
}
