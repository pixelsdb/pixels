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

import io.pixelsdb.pixels.planner.plan.physical.domain.BroadcastTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.JoinInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;

/**
 * @author hank
 * @create 2022-05-07
 */
public class BroadcastJoinInput extends JoinInput
{
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
     * Default constructor for Jackson.
     */
    public BroadcastJoinInput() { }

    public BroadcastJoinInput(long transId, BroadcastTableInfo smallTable, BroadcastTableInfo largeTable,
                              JoinInfo joinInfo, boolean partialAggregationPresent,
                              PartialAggregationInfo partialAggregationInfo, MultiOutputInfo output)
    {
        super(transId, partialAggregationPresent, partialAggregationInfo, output);
        this.smallTable = smallTable;
        this.largeTable = largeTable;
        this.joinInfo = joinInfo;
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
}
