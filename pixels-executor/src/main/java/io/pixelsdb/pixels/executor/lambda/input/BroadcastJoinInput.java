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

import io.pixelsdb.pixels.executor.lambda.domain.BroadcastTableInfo;
import io.pixelsdb.pixels.executor.lambda.domain.JoinInfo;
import io.pixelsdb.pixels.executor.lambda.domain.MultiOutputInfo;

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
     * The small (i.e., broadcast) table.
     */
    private BroadcastTableInfo smallTable;
    /**
     * The large table.
     */
    private BroadcastTableInfo largeTable;
    /**
     * The information of the broadcast join.
     */
    private JoinInfo joinInfo;
    /**
     * The information of the join output files.<br/>
     * <b>Note: </b>for inner, right-outer, and natural joins, the number of output files
     * should be consistent with the number of input splits in right table. For left-outer
     * and full-outer joins, there is an additional output file for the left-outer records.
     */
    private MultiOutputInfo output;

    /**
     * Default constructor for Jackson.
     */
    public BroadcastJoinInput() { }

    public BroadcastJoinInput(long queryId, BroadcastTableInfo smallTable,
                              BroadcastTableInfo largeTable, JoinInfo joinInfo,
                              MultiOutputInfo output)
    {
        this.queryId = queryId;
        this.smallTable = smallTable;
        this.largeTable = largeTable;
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

    public BroadcastTableInfo getSmallTable()
    {
        return smallTable;
    }

    public void setSmallTable(BroadcastTableInfo smallTable)
    {
        this.smallTable = smallTable;
    }

    public BroadcastTableInfo getLargeTable()
    {
        return largeTable;
    }

    public void setLargeTable(BroadcastTableInfo largeTable)
    {
        this.largeTable = largeTable;
    }

    public JoinInfo getJoinInfo()
    {
        return joinInfo;
    }

    public void setJoinInfo(JoinInfo joinInfo)
    {
        this.joinInfo = joinInfo;
    }

    @Override
    public MultiOutputInfo getOutput()
    {
        return output;
    }

    @Override
    public void setOutput(MultiOutputInfo output)
    {
        this.output = output;
    }
}
