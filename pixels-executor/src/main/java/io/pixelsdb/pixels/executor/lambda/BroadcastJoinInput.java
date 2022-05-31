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
package io.pixelsdb.pixels.executor.lambda;

import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.lambda.ScanInput.InputInfo;
import io.pixelsdb.pixels.executor.lambda.ScanInput.OutputInfo;

import java.util.List;

/**
 * @author hank
 * @date 07/05/2022
 */
public class BroadcastJoinInput implements JoinInput
{
    /**
     * The unique id of the query.
     */
    private long queryId;

    /**
     * The small and broadcast table.
     */
    private TableInfo leftTable;
    /**
     * The right and big table.
     */
    private TableInfo rightTable;

    /**
     * The type of the join.
     */
    private JoinType joinType;
    /**
     * The column names in the join result, in the same order of left/right cols.
     */
    private String[] joinedCols;
    /**
     * The output information of the join worker.
     */
    private OutputInfo output;
    /**
     * Whether the output has to be partitioned.
     */
    private boolean outputPartitioned = false;
    /**
     * The partition information of the output if outputPartitioned is true.
     */
    private PartitionInput.PartitionInfo outputPartitionInfo;

    /**
     * Default constructor for Jackson.
     */
    public BroadcastJoinInput() { }

    public BroadcastJoinInput(long queryId, TableInfo leftTable, TableInfo rightTable,
                              JoinType joinType, String[] joinedCols,
                              OutputInfo output, boolean outputPartitioned,
                              PartitionInput.PartitionInfo outputPartitionInfo)
    {
        this.queryId = queryId;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.joinedCols = joinedCols;
        this.output = output;
        this.outputPartitioned = outputPartitioned;
        this.outputPartitionInfo = outputPartitionInfo;
    }

    public long getQueryId()
    {
        return queryId;
    }

    public void setQueryId(long queryId)
    {
        this.queryId = queryId;
    }

    public TableInfo getLeftTable()
    {
        return leftTable;
    }

    public void setLeftTable(TableInfo leftTable)
    {
        this.leftTable = leftTable;
    }

    public TableInfo getRightTable()
    {
        return rightTable;
    }

    public void setRightTable(TableInfo rightTable)
    {
        this.rightTable = rightTable;
    }

    public JoinType getJoinType()
    {
        return joinType;
    }

    public void setJoinType(JoinType joinType)
    {
        this.joinType = joinType;
    }

    public String[] getJoinedCols()
    {
        return joinedCols;
    }

    public void setJoinedCols(String[] joinedCols)
    {
        this.joinedCols = joinedCols;
    }

    public OutputInfo getOutput()
    {
        return output;
    }

    public void setOutput(OutputInfo output)
    {
        this.output = output;
    }

    public boolean isOutputPartitioned()
    {
        return outputPartitioned;
    }

    public void setOutputPartitioned(boolean outputPartitioned)
    {
        this.outputPartitioned = outputPartitioned;
    }

    public PartitionInput.PartitionInfo getOutputPartitionInfo()
    {
        return outputPartitionInfo;
    }

    public void setOutputPartitionInfo(PartitionInput.PartitionInfo outputPartitionInfo)
    {
        this.outputPartitionInfo = outputPartitionInfo;
    }

    public static class TableInfo
    {
        private String tableName;
        /**
         * The scan inputs of the left table.
         */
        private List<InputInfo> inputs;
        /**
         * The number of row groups to be scanned in each query split of the left table.
         */
        private int splitSize;
        /**
         * The name of the columns to scan for the left table.
         */
        private String[] cols;
        /**
         * The join-key column ids of the left table.
         */
        private int[] keyColumnIds;
        /**
         * The json string of the filter (i.e., predicates) to be used in scan of the left table.
         */
        private String filter;

        /**
         * Default constructor for Jackson.
         */
        public TableInfo() { }

        public TableInfo(String tableName, List<InputInfo> inputs, int splitSize, String[] cols,
                         int[] keyColumnIds, String filter)
        {
            this.tableName = tableName;
            this.inputs = inputs;
            this.splitSize = splitSize;
            this.cols = cols;
            this.keyColumnIds = keyColumnIds;
            this.filter = filter;
        }

        public String getTableName()
        {
            return tableName;
        }

        public void setTableName(String tableName)
        {
            this.tableName = tableName;
        }

        public List<InputInfo> getInputs()
        {
            return inputs;
        }

        public void setInputs(List<InputInfo> inputs)
        {
            this.inputs = inputs;
        }

        public int getSplitSize()
        {
            return splitSize;
        }

        public void setSplitSize(int splitSize)
        {
            this.splitSize = splitSize;
        }

        public String[] getCols()
        {
            return cols;
        }

        public void setCols(String[] cols)
        {
            this.cols = cols;
        }

        public int[] getKeyColumnIds()
        {
            return keyColumnIds;
        }

        public void setKeyColumnIds(int[] keyColumnIds)
        {
            this.keyColumnIds = keyColumnIds;
        }

        public String getFilter()
        {
            return filter;
        }

        public void setFilter(String filter)
        {
            this.filter = filter;
        }
    }
}
