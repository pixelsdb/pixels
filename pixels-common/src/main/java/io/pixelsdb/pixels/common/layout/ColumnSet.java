/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.common.layout;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @author hank
 */
public class ColumnSet
{
    private Set<String> columns = new HashSet<>();

    public ColumnSet()
    {
    }

    public ColumnSet(Collection<String> columns)
    {
        this.columns.addAll(columns);
    }

    public void addColumn(String column)
    {
        this.columns.add(column);
    }

    public boolean contains(String column)
    {
        return this.columns.contains(column);
    }

    public int size()
    {
        return this.columns.size();
    }

    public boolean isEmpty ()
    {
        return this.columns.isEmpty();
    }

    public Set<String> getColumns()
    {
        return this.columns;
    }

    @Override
    public int hashCode()
    {
        return this.columns.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null)
        {
            return false;
        }
        if (o instanceof ColumnSet)
        {
            ColumnSet set = (ColumnSet) o;
            if (this.columns == null || set.columns == null)
            {
                if (this.columns == set.columns)
                {
                    return true;
                }
                return false;
            }
            if (this.columns.size() != set.columns.size())
            {
                return false;
            }
            for (String column : set.columns)
            {
                if (!this.columns.contains(column))
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException
    {
        return new ColumnSet(this.columns);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        for (String column : this.columns)
        {
            builder.append(column).append(',');
        }
        return builder.substring(0, builder.length() - 1);
    }
}
