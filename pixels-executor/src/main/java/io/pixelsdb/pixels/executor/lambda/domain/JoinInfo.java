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

import io.pixelsdb.pixels.executor.join.JoinType;

/**
 * @author hank
 * @date 02/06/2022
 */
public class JoinInfo
{
    /**
     * The type of the join.
     */
    private JoinType joinType;
    /**
     * The column names in the join result, in the same order of
     * left table columns+ right table columns. Join key columns may be ignored if
     * the {@link #joinType} is NATURAL or {@link #outputJoinKeys} is false.
     */
    private String[] resultColumns;
    /**
     * Whether the join result contains the join keys from the left and right tables.
     */
    private boolean outputJoinKeys;
    /**
     * Whether the join output has to be partitioned.
     */
    private boolean postPartition = false;
    /**
     * The partition information of the output if outputPartitioned is true.
     */
    private PartitionInfo postPartitionInfo;

    /**
     * Default constructor for Jackson.
     */
    public JoinInfo() { }

    public JoinInfo(JoinType joinType, String[] resultColumns, boolean outputJoinKeys,
                    boolean postPartition, PartitionInfo postPartitionInfo)
    {
        this.joinType = joinType;
        this.resultColumns = resultColumns;
        this.outputJoinKeys = outputJoinKeys;
        this.postPartition = postPartition;
        this.postPartitionInfo = postPartitionInfo;
    }

    public JoinType getJoinType()
    {
        return joinType;
    }

    public void setJoinType(JoinType joinType)
    {
        this.joinType = joinType;
    }

    /**
     * Get the alias of the columns in the join result. The order of the alias <b>MUST</b>
     * follow the order of the left table columns and the right table columns.
     * <p/>
     * For example, if the left table L scans 3 column A, B, and C, whereas the right table R
     * scans 4 columns D, E, F, and G. The join condition is (L inner join R on L.A=R.B).
     * Then, the joined columns would be A, B, C, D, E, F, and G. And the alias of the joined
     * columns must follow this order, such as A_0, B_1, C_2, D_3, E_4, F_5, and G_6.
     *
     * @return the alias of the columns in the join result
     */
    public String[] getResultColumns()
    {
        return resultColumns;
    }

    public void setResultColumns(String[] resultColumns)
    {
        this.resultColumns = resultColumns;
    }

    public boolean isOutputJoinKeys()
    {
        return outputJoinKeys;
    }

    public void setOutputJoinKeys(boolean outputJoinKeys)
    {
        this.outputJoinKeys = outputJoinKeys;
    }

    public boolean isPostPartition()
    {
        return postPartition;
    }

    public void setPostPartition(boolean postPartition)
    {
        this.postPartition = postPartition;
    }

    public PartitionInfo getPostPartitionInfo()
    {
        return postPartitionInfo;
    }

    public void setPostPartitionInfo(PartitionInfo postPartitionInfo)
    {
        this.postPartitionInfo = postPartitionInfo;
    }
}
