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
    private int numRowGroupInBlock;
    private int numColumn;
    private int cacheBorder;
    private List<String> columnletOrder = new ArrayList<>();

    public int getNumRowGroupInBlock()
    {
        return numRowGroupInBlock;
    }

    public void setNumRowGroupInBlock(int numRowGroupInBlock)
    {
        this.numRowGroupInBlock = numRowGroupInBlock;
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

    public List<String> getColumnletOrder()
    {
        return columnletOrder;
    }

    public void setColumnletOrder(List<String> columnletOrder)
    {
        this.columnletOrder = columnletOrder;
    }

    public void addColumnletOrder(String columnlet)
    {
        this.columnletOrder.add(columnlet);
    }
}
