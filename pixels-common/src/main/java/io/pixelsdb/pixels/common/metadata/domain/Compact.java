/*
 * Copyright 2019 PixelsDB.
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
 * @author hank
 */
public class Compact
{
    private int numRowGroupInFile;
    private int numColumn;
    private int cacheBorder;
    private List<String> columnChunkOrder = new ArrayList<>();

    public int getNumRowGroupInFile()
    {
        return numRowGroupInFile;
    }

    public void setNumRowGroupInFile(int numRowGroupInFile)
    {
        this.numRowGroupInFile = numRowGroupInFile;
    }

    public int getNumColumn()
    {
        return numColumn;
    }

    public void setNumColumn(int numColumn)
    {
        this.numColumn = numColumn;
    }

    public int getCacheBorder()
    {
        return cacheBorder;
    }

    public void setCacheBorder(int cacheBorder)
    {
        this.cacheBorder = cacheBorder;
    }

    public List<String> getColumnChunkOrder()
    {
        return columnChunkOrder;
    }

    public void setColumnChunkOrder(List<String> columnChunkOrder)
    {
        this.columnChunkOrder = columnChunkOrder;
    }

    public void addColumnChunkOrder(int rowGroupId, int columnId)
    {
        this.columnChunkOrder.add(rowGroupId + ":" + columnId);
    }
}
