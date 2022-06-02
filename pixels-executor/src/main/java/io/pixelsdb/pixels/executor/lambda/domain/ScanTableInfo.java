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
public class ScanTableInfo extends TableInfo
{
    /**
     * The json string of the filter (i.e., predicates) to be used in scan.
     */
    private String filter;

    public ScanTableInfo() { }

    public ScanTableInfo(String tableName, List<InputSplit> inputs, String[] cols, String filter)
    {
        super(tableName, inputs, cols);
        this.filter = filter;
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