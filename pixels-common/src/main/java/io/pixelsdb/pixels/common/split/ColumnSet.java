package io.pixelsdb.pixels.common.split;

import java.util.HashSet;
import java.util.Set;

public class ColumnSet
{
    private Set<String> columns = new HashSet<>();

    public ColumnSet()
    {
    }

    public ColumnSet(Set<String> columns)
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
