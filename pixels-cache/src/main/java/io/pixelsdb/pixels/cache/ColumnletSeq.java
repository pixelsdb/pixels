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
package io.pixelsdb.pixels.cache;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ColumnletSeq
{
    private final List<ColumnletId> columnlets;
    public long offset = 0;
    public int length = 0;

    public ColumnletSeq()
    {
        this.columnlets = new ArrayList<>();
    }

    public ColumnletSeq(int capacity)
    {
        this.columnlets = new ArrayList<>(capacity);
    }

    public boolean addColumnlet(ColumnletId columnlet)
    {
        if (length == 0)
        {
            columnlets.add(columnlet);
            offset = columnlet.cacheOffset;
            length += columnlet.cacheLength;
            return true;
        }
        else
        {
            if (columnlet.cacheOffset - offset - length == 0)
            {
                columnlets.add(columnlet);
                length += columnlet.cacheLength;
                return true;
            }
        }
        return false;
    }

    public List<ColumnletId> getSortedChunks()
    {
        columnlets.sort(Comparator.comparingLong(c -> c.cacheOffset));
        return columnlets;
    }
}
