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

/**
 * @author hank
 * @date 02/06/2022
 */
public class PartitionInfo
{
    /**
     * The column ids of the partition key columns.
     */
    private int[] keyColumnIds;

    /**
     * The number of partitions in the output.
     */
    private int numPartition;

    /**
     * Default constructor for Jackson.
     */
    public PartitionInfo() { }

    public PartitionInfo(int[] keyColumnIds, int numPartition)
    {
        this.keyColumnIds = keyColumnIds;
        this.numPartition = numPartition;
    }

    public int[] getKeyColumnIds()
    {
        return keyColumnIds;
    }

    public void setKeyColumnIds(int[] keyColumnIds)
    {
        this.keyColumnIds = keyColumnIds;
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