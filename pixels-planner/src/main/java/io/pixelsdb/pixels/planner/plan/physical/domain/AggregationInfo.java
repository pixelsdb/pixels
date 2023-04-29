/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.planner.plan.physical.domain;

import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;

import java.util.List;

/**
 * The information of the final aggregation.
 * @author hank
 * @create 2023-04-29 (move fields from {@link AggregationInput} to here)
 */
public class AggregationInfo
{
    /**
     * Whether the input files are partitioned.
     */
    private boolean inputPartitioned;
    /**
     * The hash values to be processed by this aggregation worker.
     */
    private List<Integer> hashValues;
    /**
     * The number of partitions in the input files.
     */
    private int numPartition;
    /**
     * The column ids of the group-key columns in columnsToRead.
     */
    private int[] groupKeyColumnIds;
    /**
     * The column ids of the aggregate columns in columnsToRead.
     */
    private int[] aggregateColumnIds;
    /**
     * The column names of the group-key columns in the aggregation result.
     */
    private String[] groupKeyColumnNames;
    /**
     * If a group-key column appears in the aggregation result,
     * the corresponding element in this array should be true, and vice versa.
     */
    private boolean[] groupKeyColumnProjection;
    /**
     * The column names of the aggregated columns in the aggregation result.
     */
    private String[] resultColumnNames;
    /**
     * The display name of the data types of the result columns.
     * They should be parsed by the TypeDescription in Pixels.
     */
    private String[] resultColumnTypes;
    /**
     * The aggregation functions, in the same order of resultColumnNames.
     */
    private FunctionType[] functionTypes;

    public AggregationInfo() { }

    public AggregationInfo(boolean inputPartitioned, List<Integer> hashValues, int numPartition,
                           int[] groupKeyColumnIds, int[] aggregateColumnIds, String[] groupKeyColumnNames,
                           boolean[] groupKeyColumnProjection, String[] resultColumnNames,
                           String[] resultColumnTypes, FunctionType[] functionTypes)
    {
        this.inputPartitioned = inputPartitioned;
        this.hashValues = hashValues;
        this.numPartition = numPartition;
        this.groupKeyColumnIds = groupKeyColumnIds;
        this.aggregateColumnIds = aggregateColumnIds;
        this.groupKeyColumnNames = groupKeyColumnNames;
        this.groupKeyColumnProjection = groupKeyColumnProjection;
        this.resultColumnNames = resultColumnNames;
        this.resultColumnTypes = resultColumnTypes;
        this.functionTypes = functionTypes;
    }

    public boolean isInputPartitioned()
    {
        return inputPartitioned;
    }

    public void setInputPartitioned(boolean inputPartitioned)
    {
        this.inputPartitioned = inputPartitioned;
    }

    public List<Integer> getHashValues()
    {
        return hashValues;
    }

    public void setHashValues(List<Integer> hashValues)
    {
        this.hashValues = hashValues;
    }

    public int getNumPartition()
    {
        return numPartition;
    }

    public void setNumPartition(int numPartition)
    {
        this.numPartition = numPartition;
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

    public String[] getGroupKeyColumnNames()
    {
        return groupKeyColumnNames;
    }

    public void setGroupKeyColumnNames(String[] groupKeyColumnNames)
    {
        this.groupKeyColumnNames = groupKeyColumnNames;
    }

    public boolean[] getGroupKeyColumnProjection()
    {
        return groupKeyColumnProjection;
    }

    public void setGroupKeyColumnProjection(boolean[] groupKeyColumnProjection)
    {
        this.groupKeyColumnProjection = groupKeyColumnProjection;
    }

    public String[] getResultColumnNames()
    {
        return resultColumnNames;
    }

    public void setResultColumnNames(String[] resultColumnNames)
    {
        this.resultColumnNames = resultColumnNames;
    }

    public String[] getResultColumnTypes()
    {
        return resultColumnTypes;
    }

    public void setResultColumnTypes(String[] resultColumnTypes)
    {
        this.resultColumnTypes = resultColumnTypes;
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
