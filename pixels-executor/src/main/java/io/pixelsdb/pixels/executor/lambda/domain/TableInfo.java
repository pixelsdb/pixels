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
public class TableInfo
{
    private String tableName;
    /**
     * The scan inputs of the table.
     */
    private List<InputSplit> inputSplits;
    /**
     * The name of the columns to read.
     */
    private String[] columnsToRead;

    /**
     * Default constructor for Jackson.
     */
    public TableInfo() { }

    public TableInfo(String tableName, List<InputSplit> inputSplits, String[] columnsToRead)
    {
        this.tableName = tableName;
        this.inputSplits = inputSplits;
        this.columnsToRead = columnsToRead;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public List<InputSplit> getInputSplits()
    {
        return inputSplits;
    }

    public void setInputSplits(List<InputSplit> inputSplits)
    {
        this.inputSplits = inputSplits;
    }

    public String[] getColumnsToRead()
    {
        return columnsToRead;
    }

    public void setColumnsToRead(String[] columnsToRead)
    {
        this.columnsToRead = columnsToRead;
    }
}
