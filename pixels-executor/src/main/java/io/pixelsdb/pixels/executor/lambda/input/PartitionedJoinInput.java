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
package io.pixelsdb.pixels.executor.lambda.input;

import io.pixelsdb.pixels.executor.lambda.domain.MultiOutputInfo;
import io.pixelsdb.pixels.executor.lambda.domain.PartitionedJoinInfo;
import io.pixelsdb.pixels.executor.lambda.domain.PartitionedTableInfo;

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
    /**
     * The information of the left partitioned table.
     */
    private PartitionedTableInfo leftTable;
    /**
     * The information of the right partitioned table.
     */
    private PartitionedTableInfo rightTable;
    /**
     * The information of the partitioned join.
     */
    private PartitionedJoinInfo joinInfo;
    /**
     * The information of the join output files.<br/>
     * <b>Note: </b>for inner, right-outer, and natural joins, the number of output files
     * should be consistent with the parallelism of the right table. For left-outer and
     * full-outer joins, there is an additional output file for the left-outer records.
     */
    private MultiOutputInfo output;

    /**
     * Default constructor for Jackson.
     */
    public PartitionedJoinInput() { }

    public PartitionedJoinInput(long queryId, PartitionedTableInfo leftTable,
                                PartitionedTableInfo rightTable, PartitionedJoinInfo joinInfo,
                                MultiOutputInfo output)
    {
        this.queryId = queryId;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinInfo = joinInfo;
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

    public PartitionedTableInfo getLeftTable()
    {
        return leftTable;
    }

    public void setLeftTable(PartitionedTableInfo leftTable)
    {
        this.leftTable = leftTable;
    }

    public PartitionedTableInfo getRightTable()
    {
        return rightTable;
    }

    public void setRightTable(PartitionedTableInfo rightTable)
    {
        this.rightTable = rightTable;
    }

    public PartitionedJoinInfo getJoinInfo()
    {
        return joinInfo;
    }

    public void setJoinInfo(PartitionedJoinInfo joinInfo)
    {
        this.joinInfo = joinInfo;
    }

    @Override
    public MultiOutputInfo getOutput()
    {
        return output;
    }

    public void setOutput(MultiOutputInfo output)
    {
        this.output = output;
    }
}
