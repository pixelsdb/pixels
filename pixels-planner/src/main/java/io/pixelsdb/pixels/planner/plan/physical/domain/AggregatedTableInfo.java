/*
 * Copyright 2023 PixelsDB.
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

import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;

import java.util.List;

/**
 * The partial aggregates are seen as a table (i.e., materialized view). This is the
 * information of the partial aggregates.
 * @author hank
 * @create 2023-04-29 (move fields from {@link AggregationInput} to here)
 */
public class AggregatedTableInfo extends TableInfo
{
    /**
     * The paths of the partial aggregated files.
     */
    private List<String> inputFiles;
    /**
     * The number of threads to scan and aggregate the input files.
     */
    private int parallelism;

    public AggregatedTableInfo() { }

    public AggregatedTableInfo(String tableName, boolean base, String[] columnsToRead,
                               StorageInfo storageInfo, List<String> inputFiles, int parallelism)
    {
        super(tableName, base, columnsToRead, storageInfo);
        this.inputFiles = inputFiles;
        this.parallelism = parallelism;
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

}
