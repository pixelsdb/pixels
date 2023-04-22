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
package io.pixelsdb.pixels.planner.plan.physical.output;

import io.pixelsdb.pixels.common.turbo.Output;

import java.util.Set;

/**
 * The output format of the hash partitioning.
 * @author hank
 * @date 07/05/2022
 */
public class PartitionOutput extends Output
{
    /**
     * The path of the partitioned file.
     */
    private String path;

    /**
     * The hash value of the partitions that exist in the partitioned file.
     */
    private Set<Integer> hashValues;

    /**
     * Default constructor for Jackson.
     */
    public PartitionOutput() { }

    public PartitionOutput(String path, Set<Integer> hashValues)
    {
        this.path = path;
        this.hashValues = hashValues;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public Set<Integer> getHashValues()
    {
        return hashValues;
    }

    public void setHashValues(Set<Integer> hashValues)
    {
        this.hashValues = hashValues;
    }
}
