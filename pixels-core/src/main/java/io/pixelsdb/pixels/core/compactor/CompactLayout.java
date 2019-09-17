/*
 * Copyright 2017-2019 PixelsDB.
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
package io.pixelsdb.pixels.core.compactor;

import io.pixelsdb.pixels.common.metadata.domain.Compact;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 */
public class CompactLayout
{
    private int rowGroupNumber = 0;
    private int columnNumber = 0;
    private List<ColumnletIndex> indices = null;

    protected CompactLayout(int rowGroupNumber, int columnNumber)
    {
        this.rowGroupNumber = rowGroupNumber;
        this.columnNumber = columnNumber;
        this.indices = new ArrayList<>(rowGroupNumber * columnNumber);
    }

    public static CompactLayout fromCompact(Compact compact)
    {
        CompactLayout layout = new CompactLayout(compact.getNumRowGroupInBlock(), compact.getNumColumn());
        for (String columnletStr : compact.getColumnletOrder())
        {
            String[] splits = columnletStr.split(":");
            int rowGroupId = Integer.parseInt(splits[0]);
            int columnId = Integer.parseInt(splits[1]);
            layout.append(rowGroupId, columnId);
        }
        return layout;
    }

    public static CompactLayout buildNaive(int rowGroupNumber, int columnNumber)
    {
        CompactLayout layout = new CompactLayout(rowGroupNumber, columnNumber);
        for (int i = 0; i < rowGroupNumber; i++)
        {
            for (int j = 0; j < columnNumber; j++)
            {
                layout.append(i, j);
            }
        }
        return layout;
    }

    protected void append(int rowGroupId, int columnId)
    {
        this.indices.add(new ColumnletIndex(rowGroupId, columnId));
    }

    public int size()
    {
        return rowGroupNumber * columnNumber;
    }

    public ColumnletIndex get(int i)
    {
        return this.indices.get(i);
    }
}
