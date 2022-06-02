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

import java.util.List;

/**
 * @author hank
 * @date 02/06/2022
 */
public class PartitionedTableInfo
{
    private String tableName;
    /**
     * The partitioned file paths of the table.
     */
    private List<String> inputFiles;
    /**
     * The number of threads used to process the table.
     */
    private int parallelism;
    /**
     * The name of the columns to read.
     */
    private String[] columnsToRead;
    /**
     * The ids of the join-key columns of the table.
     */
    private int[] keyColumnIds;

    /**
     * Default constructor for Jackson.
     */
    public PartitionedTableInfo() { }

    public PartitionedTableInfo(String tableName, List<String> inputFiles,
                                int parallelism, String[] columnsToRead, int[] keyColumnIds)
    {
        this.tableName = tableName;
        this.inputFiles = inputFiles;
        this.parallelism = parallelism;
        this.columnsToRead = columnsToRead;
        this.keyColumnIds = keyColumnIds;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
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

    public String[] getColumnsToRead()
    {
        return columnsToRead;
    }

    public void setColumnsToRead(String[] columnsToRead)
    {
        this.columnsToRead = columnsToRead;
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
