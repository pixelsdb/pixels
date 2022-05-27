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
import io.pixelsdb.pixels.executor.lambda.BroadcastJoinInput.TableInfo;
import io.pixelsdb.pixels.executor.lambda.ScanInput.OutputInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 * @date 26/05/2022
 */
public class ChainBroadcastJoinInput implements JoinInput
{
    /**
     * The unique id of the query.
     */
    private long queryId;

    /**
     * The far left table in the join chain.
     */
    private TableInfo leftTable;
    /**
     * The chain of the broadcast tables between the left and the right tables.
     * These tables are joined following their order in the list.
     * Start joins are not supported here.
     */
    private List<ChainTableInfo> chainTables;
    /**
     * The right (big) table.
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
     * Default constructor for Jackson.
     */
    public ChainBroadcastJoinInput()
    {
        this.chainTables = new ArrayList<>();
    }

    public ChainBroadcastJoinInput(long queryId, TableInfo leftTable, List<ChainTableInfo> chainTables,
                                   TableInfo rightTable, JoinType joinType, String[] joinedCols, OutputInfo output)
    {
        this.queryId = queryId;
        this.leftTable = leftTable;
        this.chainTables = chainTables;
        this.rightTable = rightTable;
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

    public TableInfo getLeftTable()
    {
        return leftTable;
    }

    public void setLeftTable(TableInfo leftTable)
    {
        this.leftTable = leftTable;
    }

    public List<ChainTableInfo> getChainTables()
    {
        return chainTables;
    }

    public void setChainTables(List<ChainTableInfo> chainTables)
    {
        this.chainTables = chainTables;
    }

    public void addChainTable(ChainTableInfo chainTable)
    {
        this.chainTables.add(chainTable);
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

    /**
     * The chain table between the left and right table.
     */
    public static class ChainTableInfo extends TableInfo
    {
        private int[] secondKeyColumnIds;

        public ChainTableInfo() { }

        public ChainTableInfo(String tableName, List<ScanInput.InputInfo> inputs, int splitSize,
                              String[] cols, int[] keyColumnIds, String filter, int[] secondKeyColumnIds)
        {
            super(tableName, inputs, splitSize, cols, keyColumnIds, filter);
            this.secondKeyColumnIds = secondKeyColumnIds;
        }

        public int[] getSecondKeyColumnIds()
        {
            return secondKeyColumnIds;
        }

        public void setSecondKeyColumnIds(int[] secondKeyColumnIds)
        {
            this.secondKeyColumnIds = secondKeyColumnIds;
        }
    }
}
