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

import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;

/**
 * The input format for hash partitioning.
 * Hash partitioner is also responsible for projection and filtering, thus
 * hash partitioning input shares some fields with the scan input.
 *
 * @author hank
 * @date 07/05/2022
 */
public class PartitionInput extends Input
{
    /**
     * The unique id of the query.
     */
    private long queryId;
    /**
     * The information of the table to scan and partition.
     */
    private ScanTableInfo tableInfo;
    /**
     * If a column from the table's columnToRead appears in the partition output,
     * the corresponding element in this array would be true, and vice versa.
     */
    private boolean[] projection;
    /**
     * The information of the partition output.
     */
    private OutputInfo output;
    /**
     * The information about the hash partitioning.
     */
    private PartitionInfo partitionInfo;

    /**
     * Default constructor for Jackson.
     */
    public PartitionInput() { }

    public PartitionInput(long queryId, ScanTableInfo tableInfo, boolean[] projection,
                          OutputInfo output, PartitionInfo partitionInfo)
    {
        this.queryId = queryId;
        this.tableInfo = tableInfo;
        this.projection = projection;
        this.output = output;
        this.partitionInfo = partitionInfo;
    }

    public long getQueryId()
    {
        return queryId;
    }

    public void setQueryId(long queryId)
    {
        this.queryId = queryId;
    }

    public ScanTableInfo getTableInfo()
    {
        return tableInfo;
    }

    public void setTableInfo(ScanTableInfo tableInfo)
    {
        this.tableInfo = tableInfo;
    }

    public boolean[] getProjection()
    {
        return projection;
    }

    public void setProjection(boolean[] projection)
    {
        this.projection = projection;
    }

    public OutputInfo getOutput()
    {
        return output;
    }

    public void setOutput(OutputInfo output)
    {
        this.output = output;
    }

    public PartitionInfo getPartitionInfo()
    {
        return partitionInfo;
    }

    public void setPartitionInfo(PartitionInfo partitionInfo)
    {
        this.partitionInfo = partitionInfo;
    }
}
