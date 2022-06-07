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

import java.util.List;

/**
 * @author hank
 * @date 02/06/2022
 */
public class PartitionedJoinInfo extends JoinInfo
{
    /**
     * The total number of partitions.
     */
    int numPartition;
    /**
     * The hash values to be processed by a hash join worker.
     */
    private List<Integer> hashValues;

    /**
     * Default constructor for Jackson.
     */
    public PartitionedJoinInfo() { }

    public PartitionedJoinInfo(JoinType joinType, String[] smallColumnAlias, String[] largeColumnAlias,
                               boolean outputJoinKeys, boolean postPartition, PartitionInfo postPartitionInfo,
                               int numPartition, List<Integer> hashValues)
    {
        super(joinType, smallColumnAlias, largeColumnAlias, outputJoinKeys, postPartition, postPartitionInfo);
        this.numPartition = numPartition;
        this.hashValues = hashValues;
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
}
