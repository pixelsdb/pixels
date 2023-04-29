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

import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedJoinInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;

/**
 * @author hank
 * @create 2022-05-07
 */
public class PartitionedJoinInput extends JoinInput
{
    /**
     * The information of the small partitioned table.
     */
    private PartitionedTableInfo smallTable;
    /**
     * The information of the large partitioned table.
     */
    private PartitionedTableInfo largeTable;
    /**
     * The information of the partitioned join.
     */
    private PartitionedJoinInfo joinInfo;

    /**
     * Default constructor for Jackson.
     */
    public PartitionedJoinInput() { }

    public PartitionedJoinInput(long queryId, PartitionedTableInfo smallTable, PartitionedTableInfo largeTable,
                                PartitionedJoinInfo joinInfo, boolean partialAggregationPresent,
                                PartialAggregationInfo partialAggregationInfo, MultiOutputInfo output)
    {
        super(queryId, partialAggregationPresent, partialAggregationInfo, output);
        this.smallTable = smallTable;
        this.largeTable = largeTable;
        this.joinInfo = joinInfo;
    }

    public PartitionedTableInfo getSmallTable()
    {
        return smallTable;
    }

    public void setSmallTable(PartitionedTableInfo smallTable)
    {
        this.smallTable = smallTable;
    }

    public PartitionedTableInfo getLargeTable()
    {
        return largeTable;
    }

    public void setLargeTable(PartitionedTableInfo largeTable)
    {
        this.largeTable = largeTable;
    }

    public PartitionedJoinInfo getJoinInfo()
    {
        return joinInfo;
    }

    public void setJoinInfo(PartitionedJoinInfo joinInfo)
    {
        this.joinInfo = joinInfo;
    }
}
