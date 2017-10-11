package cn.edu.ruc.iir.rainbow.layout.domian;

import java.util.HashSet;
import java.util.Set;

public class Query
{
    private int id;
    private String sid;
    private double weight;
    private Set<Integer> columnIds;

    public Query(int id, String sid, double weight)
    {
        this.id = id;
        this.sid = sid;
        this.weight = weight;
        this.columnIds = new HashSet<Integer>();
    }

    public Query(int id, String sid, double weight, Set<Integer> columnIds)
    {
        this.id = id;
        this.sid = sid;
        this.weight = weight;
        this.columnIds = columnIds;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Query)
        {
            Query c = (Query) obj;
            return c.id == this.id;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return this.id;
    }

    public void addColumnId(int cid)
    {
        this.columnIds.add(cid);
    }

    public int getId()
    {
        return id;
    }

    public String getSid () { return sid; }

    public void addWeight (double weight)
    {
        this.weight += weight;
    }

    public double getWeight()
    {
        return weight;
    }

    public Set<Integer> getColumnIds()
    {
        return columnIds;
    }

    public boolean hasColumnId (int cid)
    {
        return this.columnIds.contains(cid);
    }

    @Override
    public Object clone()
    {
        return this;
    }
}
