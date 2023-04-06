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
package io.pixelsdb.pixels.turbo.planner.plan.physical.domain;

import java.util.List;

/**
 * @author hank
 * @date 02/06/2022
 */
public class ScanTableInfo extends TableInfo
{
    /**
     * The scan inputs of the table.
     */
    private List<InputSplit> inputSplits;
    /**
     * The json string of the filter (i.e., predicates) to be used in scan.
     */
    private String filter;

    /**
     * Default constructor for Jackson.
     */
    public ScanTableInfo() { }

    public ScanTableInfo(String tableName, boolean base, List<InputSplit> inputSplits,
                         String[] columnsToRead, String filter)
    {
        super(tableName, base, columnsToRead);
        this.inputSplits = inputSplits;
        this.filter = filter;
    }

    public List<InputSplit> getInputSplits()
    {
        return inputSplits;
    }

    public void setInputSplits(List<InputSplit> inputSplits)
    {
        this.inputSplits = inputSplits;
    }

    public String getFilter()
    {
        return filter;
    }

    public void setFilter(String filter)
    {
        this.filter = filter;
    }
}