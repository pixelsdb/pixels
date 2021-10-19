/*
 * Copyright 2021 PixelsDB.
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
package io.pixelsdb.pixels.common.metadata.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * Created at: 10/19/21
 * Author: hank
 */
public class ProjectionPattern
{
    private List<Integer> accessedColumns = new ArrayList<>();
    private String path;

    public List<Integer> getAccessedColumns()
    {
        return accessedColumns;
    }

    public void setAccessedColumns(List<Integer> accessedColumns)
    {
        this.accessedColumns = accessedColumns;
    }


    public void addAccessedColumns(int accessedColumn)
    {
        this.accessedColumns.add(accessedColumn);
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }
}
