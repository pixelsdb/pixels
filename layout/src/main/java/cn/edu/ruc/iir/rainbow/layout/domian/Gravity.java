package cn.edu.ruc.iir.rainbow.layout.domian;

import java.util.Set;

/**
 * Created by hank on 2015/9/9.
 */
public class Gravity implements Comparable<Gravity>
{
    private Column column = null;
    private Set<Query> qieries = null;
    private double seekCost = 0;
    private double location = 0;

    public Gravity(Column column, Set<Query> qieries, double seekCost, double location)
    {
        this.column = column;
        this.qieries = qieries;
        this.seekCost = seekCost;
        this.location = location;
    }

    public Column getColumn()
    {
        return column;
    }

    public void setColumn(Column column)
    {
        this.column = column;
    }

    public Set<Query> getQieries()
    {
        return qieries;
    }

    public void setQieries(Set<Query> qieries)
    {
        this.qieries = qieries;
    }

    public double getSeekCost()
    {
        return seekCost;
    }

    public void setSeekCost(double seekCost)
    {
        this.seekCost = seekCost;
    }

    public double getLocation()
    {
        return location;
    }

    public void setLocation(double location)
    {
        this.location = location;
    }

    @Override
    public int compareTo(Gravity g)
    {
        if (this.seekCost < g.seekCost)
        {
            return 1;
        }
        else if (this.seekCost > g.seekCost)
        {
            return -1;
        }
        return 0;
    }
}
