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
package io.pixelsdb.pixels.planner.plan.physical.input;

import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;

public class SortInput extends Input
{
    /**
     * The information of the table to scan and sort.
     */
    private ScanTableInfo tableInfo;
    /**
     * If a column from the table's columnToRead appears in the sort output,
     * the corresponding element in this array would be true, and vice versa.
     */
    private boolean[] projection;
    private OutputInfo output;
    private int[] keyColumnIds;
    private int numPartition;

    private boolean isSorted;

    /**
     * Default constructor for Jackson.
     */
    public SortInput()
    {
        super(-1);
    }

    public SortInput(long transId, ScanTableInfo tableInfo, boolean[] projection,
                     OutputInfo output, int[] keyColumnIds, boolean isSorted)
    {
        super(transId);
        this.tableInfo = tableInfo;
        this.projection = projection;
        this.output = output;
        this.keyColumnIds = keyColumnIds;
        this.isSorted = isSorted;
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

    public int[] getKeyColumnIds()
    {
        return keyColumnIds;
    }

    public void setKeyColumnIds(int[] keyColumnIds)
    {
        this.keyColumnIds = keyColumnIds;
    }

    public boolean isSorted()
    {
        return isSorted;
    }

    public void setIsSorted(boolean sorted)
    {
        isSorted = sorted;
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