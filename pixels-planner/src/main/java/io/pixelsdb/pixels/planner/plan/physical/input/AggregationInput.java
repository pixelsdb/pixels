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
package io.pixelsdb.pixels.planner.plan.physical.input;

import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;

import java.util.List;

/**
 * The input for the final aggregation.
 *
 * @author hank
 * @date 05/07/2022
 */
public class AggregationInput extends Input
{
    private long queryId;
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
     * The name of the columns to read from the input files.
     */
    private String[] columnsToRead;
    /**
     * The column ids of the group-key columns in {@link #columnsToRead}.
     */
    private int[] groupKeyColumnIds;
    /**
     * The column ids of the aggregate columns in {@link #columnsToRead}.
     */
    private int[] aggregateColumnIds;
    /**
     * The column names of the group-key columns in the aggregation result.
     */
    private String[] groupKeyColumnNames;
    /**
     * If a group-key column appears in the aggregation output,
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
    /**
     * The paths of the partial aggregated files.
     */
    private List<String> inputFiles;
    /**
     * The information of the input storage.
     */
    private StorageInfo inputStorage;
    /**
     * The number of threads to scan and aggregate the input files.
     */
    private int parallelism;

    /**
     * The output of the aggregation.
     */
    private OutputInfo output;

    /**
     * Default constructor for Jackson.
     */
    public AggregationInput() { }

    public AggregationInput(long queryId, boolean inputPartitioned, List<Integer> hashValues, int numPartition,
                            String[] columnsToRead, int[] groupKeyColumnIds, int[] aggregateColumnIds,
                            String[] groupKeyColumnNames, boolean[] groupKeyColumnProjection, String[] resultColumnNames,
                            String[] resultColumnTypes, FunctionType[] functionTypes, List<String> inputFiles,
                            StorageInfo inputStorage, int parallelism, OutputInfo output)
    {
        this.queryId = queryId;
        this.inputPartitioned = inputPartitioned;
        this.hashValues = hashValues;
        this.numPartition = numPartition;
        this.columnsToRead = columnsToRead;
        this.groupKeyColumnIds = groupKeyColumnIds;
        this.aggregateColumnIds = aggregateColumnIds;
        this.groupKeyColumnNames = groupKeyColumnNames;
        this.groupKeyColumnProjection = groupKeyColumnProjection;
        this.resultColumnNames = resultColumnNames;
        this.resultColumnTypes = resultColumnTypes;
        this.functionTypes = functionTypes;
        this.inputFiles = inputFiles;
        this.inputStorage = inputStorage;
        this.parallelism = parallelism;
        this.output = output;
    }

    public long getQueryId()
    {
        return queryId;
    }

    public void setQueryId(long queryId)
    {
        this.queryId = queryId;
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

    public String[] getColumnsToRead()
    {
        return columnsToRead;
    }

    public void setColumnsToRead(String[] columnsToRead)
    {
        this.columnsToRead = columnsToRead;
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

    public List<String> getInputFiles()
    {
        return inputFiles;
    }

    public void setInputFiles(List<String> inputFiles)
    {
        this.inputFiles = inputFiles;
    }

    public StorageInfo getInputStorage()
    {
        return inputStorage;
    }

    public void setInputStorage(StorageInfo inputStorage)
    {
        this.inputStorage = inputStorage;
    }

    public int getParallelism()
    {
        return parallelism;
    }

    public void setParallelism(int parallelism)
    {
        this.parallelism = parallelism;
    }

    public OutputInfo getOutput()
    {
        return output;
    }

    public void setOutput(OutputInfo output)
    {
        this.output = output;
    }
}
