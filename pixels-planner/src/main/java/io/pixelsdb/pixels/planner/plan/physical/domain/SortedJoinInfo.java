/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.planner.plan.physical.domain;

import io.pixelsdb.pixels.executor.join.JoinType;


public class SortedJoinInfo extends JoinInfo
{
    int numPartition;

    /**
     * Default constructor for Jackson.
     */
    public SortedJoinInfo()
    {
    }

    public SortedJoinInfo(JoinType joinType, String[] smallColumnAlias, String[] largeColumnAlias,
                          boolean[] smallProjection, boolean[] largeProjection, boolean postPartition,
                          PartitionInfo postPartitionInfo, int numPartition)
    {
        super(joinType, smallColumnAlias, largeColumnAlias, smallProjection, largeProjection,
                postPartition, postPartitionInfo);
        this.numPartition = numPartition;
    }

    public int getNumPartition()
    {
        return numPartition;
    }

    public void setNumPartition(int numPartition)
    {
        this.numPartition = numPartition;
    }
}
