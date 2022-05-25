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
import io.pixelsdb.pixels.executor.lambda.ScanInput.OutputInfo;

import java.util.List;

/**
 * @author hank
 * @date 07/05/2022
 */
public class PartitionedJoinInput implements JoinInput
{
    /**
     * The unique id of the query.
     */
    private long queryId;

    private String leftTableName;
    /**
     * The partitioned files of the left (small) table.
     */
    private List<PartitionOutput> leftPartitioned;
    /**
     * The column names of the left table.
     */
    private String[] leftCols;
    /**
     * The ids of the join-key columns of the left table.
     */
    private int[] leftKeyColumnIds;

    private String rightTableName;
    /**
     * The partitioned files of the right (big) table.
     */
    private List<PartitionOutput> rightPartitioned;
    /**
     * The column names of the right table.
     */
    private String[] rightCols;
    /**
     * The ids of the join-key columns of the right table.
     */
    private int[] rightKeyColumnIds;

    /**
     * The total number of partitions.
     */
    int numPartition;
    /**
     * The hash values to be processed by a hash join worker.
     */
    private List<Integer> hashValues;
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
     * Default constructor for Jackson.
     */
    public PartitionedJoinInput() { }

    public PartitionedJoinInput(long queryId,
                                String leftTableName, List<PartitionOutput> leftPartitioned,
                                String[] leftCols, int[] leftKeyColumnIds,
                                String rightTableName, List<PartitionOutput> rightPartitioned,
                                String[] rightCols, int[] rightKeyColumnIds,
                                int numPartition, List<Integer> hashValues, JoinType joinType,
                                String[] joinedCols, OutputInfo output)
    {
        this.queryId = queryId;
        this.leftTableName = leftTableName;
        this.leftPartitioned = leftPartitioned;
        this.leftCols = leftCols;
        this.leftKeyColumnIds = leftKeyColumnIds;
        this.rightTableName = rightTableName;
        this.rightPartitioned = rightPartitioned;
        this.rightCols = rightCols;
        this.rightKeyColumnIds = rightKeyColumnIds;
        this.numPartition = numPartition;
        this.hashValues = hashValues;
        this.joinType = joinType;
        this.joinedCols = joinedCols;
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

    public String getLeftTableName()
    {
        return leftTableName;
    }

    public void setLeftTableName(String leftTableName)
    {
        this.leftTableName = leftTableName;
    }

    public List<PartitionOutput> getLeftPartitioned()
    {
        return leftPartitioned;
    }

    public void setLeftPartitioned(List<PartitionOutput> leftPartitioned)
    {
        this.leftPartitioned = leftPartitioned;
    }

    public String[] getLeftCols()
    {
        return leftCols;
    }

    public void setLeftCols(String[] leftCols)
    {
        this.leftCols = leftCols;
    }

    public int[] getLeftKeyColumnIds()
    {
        return leftKeyColumnIds;
    }

    public void setLeftKeyColumnIds(int[] leftKeyColumnIds)
    {
        this.leftKeyColumnIds = leftKeyColumnIds;
    }

    public String getRightTableName()
    {
        return rightTableName;
    }

    public void setRightTableName(String rightTableName)
    {
        this.rightTableName = rightTableName;
    }

    public List<PartitionOutput> getRightPartitioned()
    {
        return rightPartitioned;
    }

    public void setRightPartitioned(List<PartitionOutput> rightPartitioned)
    {
        this.rightPartitioned = rightPartitioned;
    }

    public String[] getRightCols()
    {
        return rightCols;
    }

    public void setRightCols(String[] rightCols)
    {
        this.rightCols = rightCols;
    }

    public int[] getRightKeyColumnIds()
    {
        return rightKeyColumnIds;
    }

    public void setRightKeyColumnIds(int[] rightKeyColumnIds)
    {
        this.rightKeyColumnIds = rightKeyColumnIds;
    }

    public int getNumPartition()
    {
        return numPartition;
    }

    public void setNumPartition(int numPartition)
    {
        this.numPartition = numPartition;
    }

    public List<Integer> getHashValues()
    {
        return hashValues;
    }

    public void setHashValues(List<Integer> hashValues)
    {
        this.hashValues = hashValues;
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
}
