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

    private String leftTableName;
    /**
     * The scan inputs of the left table.
     */
    private List<InputInfo> leftInputs;
    /**
     * The number of row groups to be scanned in each query split of the left table.
     */
    private int leftSplitSize;
    /**
     * The name of the columns to scan for the left table.
     */
    private String[] leftCols;
    /**
     * The join-key column ids of the left table.
     */
    private int[] leftKeyColumnIds;
    /**
     * The json string of the filter (i.e., predicates) to be used in scan of the left table.
     */
    private String leftFilter;

    private String rightTableName;
    /**
     * The scan inputs of the right table.
     */
    private List<InputInfo> rightInputs;
    /**
     * The number of row groups to be scanned in each query split of the right table.
     */
    private int rightSplitSize;
    /**
     * The name of the columns to scan for the right table.
     */
    private String[] rightCols;
    /**
     * The join-key column ids of the right table.
     */
    private int[] rightKeyColumnIds;
    /**
     * The json string of the filter (i.e., predicates) to be used in the scan of the right table.
     */
    private String rightFilter;

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
    public BroadcastJoinInput() { }

    public BroadcastJoinInput(long queryId, String leftTableName, String rightTableName,
                              List<InputInfo> leftInputs, int leftSplitSize,
                              String[] leftCols, int[] leftKeyColumnIds, String leftFilter,
                              List<InputInfo> rightInputs, int rightSplitSize,
                              String[] rightCols, int[] rightKeyColumnIds, String rightFilter,
                              JoinType joinType, String[] joinedCols, OutputInfo output)
    {
        this.queryId = queryId;
        this.leftTableName = leftTableName;
        this.rightTableName = rightTableName;
        this.leftInputs = leftInputs;
        this.leftSplitSize = leftSplitSize;
        this.leftCols = leftCols;
        this.leftKeyColumnIds = leftKeyColumnIds;
        this.leftFilter = leftFilter;
        this.rightInputs = rightInputs;
        this.rightSplitSize = rightSplitSize;
        this.rightCols = rightCols;
        this.rightKeyColumnIds = rightKeyColumnIds;
        this.rightFilter = rightFilter;
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

    public String getRightTableName()
    {
        return rightTableName;
    }

    public void setRightTableName(String rightTableName)
    {
        this.rightTableName = rightTableName;
    }

    public List<InputInfo> getLeftInputs()
    {
        return leftInputs;
    }

    public void setLeftInputs(List<InputInfo> leftInputs)
    {
        this.leftInputs = leftInputs;
    }

    public int getLeftSplitSize()
    {
        return leftSplitSize;
    }

    public void setLeftSplitSize(int leftSplitSize)
    {
        this.leftSplitSize = leftSplitSize;
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

    public String getLeftFilter()
    {
        return leftFilter;
    }

    public void setLeftFilter(String leftFilter)
    {
        this.leftFilter = leftFilter;
    }

    public List<InputInfo> getRightInputs()
    {
        return rightInputs;
    }

    public void setRightInputs(List<InputInfo> rightInputs)
    {
        this.rightInputs = rightInputs;
    }

    public int getRightSplitSize()
    {
        return rightSplitSize;
    }

    public void setRightSplitSize(int rightSplitSize)
    {
        this.rightSplitSize = rightSplitSize;
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

    public String getRightFilter()
    {
        return rightFilter;
    }

    public void setRightFilter(String rightFilter)
    {
        this.rightFilter = rightFilter;
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
