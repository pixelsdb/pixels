package cn.edu.ruc.iir.rainbow.layout.domian;

import java.util.HashSet;
import java.util.Set;

public class Column implements Comparable<Column>
{
    private int id;
    private int dupId;
    private String name;
    private String type;
    private double size;
    private boolean duplicated = false;
    private Set<Integer> queryIds = null;

    @Override
    public Column clone()
    {
        Column column = new Column(this.id, this.name, this.type, this.size);
        column.setDupId(this.dupId);
        column.setDuplicated(this.duplicated);
        column.setQueryIds(new HashSet<>(this.queryIds));
        return column;
    }

    public Column(int id, String name, String type, double size)
    {
        this.id = id;
        this.name = name;
        this.type = type;
        this.size = size;
        this.dupId = 0;
        this.duplicated = false;
        queryIds = new HashSet<>();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Column)
        {
            Column c = (Column) obj;
            if (c.duplicated == this.duplicated)
            {
                if (this.duplicated == true)
                {
                    return c.id == this.id && c.dupId == c.dupId;
                }
                else
                {
                    return c.id == this.id;
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return this.id;
    }

    public int getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public double getSize()
    {
        return size;
    }

    public boolean isDuplicated()
    {
        return duplicated;
    }

    public void setDuplicated(boolean duplicated)
    {
        this.duplicated = duplicated;
    }

    public int getDupId()
    {
        return dupId;
    }

    public void setDupId(int dupId)
    {
        this.dupId = dupId;
    }

    public void addQueryId (Integer queryId)
    {
        this.queryIds.add(queryId);
    }

    public void setQueryIds(Set<Integer> queryIds)
    {
        this.queryIds = queryIds;
    }

    public boolean hasQueryId (Integer queryId)
    {
        return this.queryIds.contains(queryId);
    }

    @Override
    public int compareTo(Column c)
    {
        return this.id - c.id;
    }
}
