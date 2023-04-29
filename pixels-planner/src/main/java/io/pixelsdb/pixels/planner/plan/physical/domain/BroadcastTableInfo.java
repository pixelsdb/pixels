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
package io.pixelsdb.pixels.planner.plan.physical.domain;

import java.util.List;

/**
 * @author hank
 * @dacreatete 2022-06-02
 */
public class BroadcastTableInfo extends ScanTableInfo
{
    /**
     * The ids of the join-key columns of the table.
     */
    private int[] keyColumnIds;

    /**
     * Default constructor for Jackson.
     */
    public BroadcastTableInfo() { }

    public BroadcastTableInfo(String tableName, boolean base, String[] columnsToRead,
                              StorageInfo storageInfo, List<InputSplit> inputSplits,
                              String filter, int[] keyColumnIds)
    {
        super(tableName, base, columnsToRead, storageInfo, inputSplits, filter);
        this.keyColumnIds = keyColumnIds;
    }

    public int[] getKeyColumnIds()
    {
        return keyColumnIds;
    }

    public void setKeyColumnIds(int[] keyColumnIds)
    {
        this.keyColumnIds = keyColumnIds;
    }
}