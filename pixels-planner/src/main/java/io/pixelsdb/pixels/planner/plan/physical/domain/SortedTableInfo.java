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

import java.util.List;

public class SortedTableInfo extends TableInfo
{
    /**
     * The sorted file paths of the table.
     */
    private List<String> inputFiles;
    /**
     * The number of threads used to process the table.
     */
    private int parallelism;

    /**
     * The ids of the join-key columns of the table.
     */
    private int[] keyColumnIds;

    /**
     * Default constructor for Jackson.
     */
    public SortedTableInfo()
    {
    }

    public SortedTableInfo(String tableName, boolean base, String[] columnsToRead,
                           StorageInfo storageInfo, List<String> inputFiles,
                           int parallelism, int[] keyColumnIds)
    {
        super(tableName, base, columnsToRead, storageInfo);
        this.inputFiles = inputFiles;
        this.parallelism = parallelism;
        this.keyColumnIds = keyColumnIds;
    }

    public List<String> getInputFiles()
    {
        return inputFiles;
    }

    public void setInputFiles(List<String> inputFiles)
    {
        this.inputFiles = inputFiles;
    }

    public int getParallelism()
    {
        return parallelism;
    }

    public void setParallelism(int parallelism)
    {
        this.parallelism = parallelism;
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
