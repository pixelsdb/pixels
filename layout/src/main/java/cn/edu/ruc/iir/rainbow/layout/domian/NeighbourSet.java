package cn.edu.ruc.iir.rainbow.layout.domian;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by hank on 2015/9/9.
 */
public class NeighbourSet
{
    private Map<Column, NeighbourInfo> neighbours = null;
    private Map<Integer, Column> locationToColumn = null;

    public NeighbourSet ()
    {
        this.neighbours = new HashMap<>();
        this.locationToColumn = new HashMap<>();
    }

    public void addNeighbour (Column column, Query query, int location, int length)
    {
        if (this.neighbours.containsKey(column))
        {
            this.neighbours.get(column).addQuery(query);
        }
        else
        {
            this.locationToColumn.put(location, column);
            NeighbourInfo info = new NeighbourInfo(location, length, query);
            this.neighbours.put(column, info);
        }
    }

    public Column getColumn (int location)
    {
        return this.locationToColumn.get(location);
    }

    public Set<Query> getQueries (int location)
    {
        Column column = this.locationToColumn.get(location);
        NeighbourInfo info = this.neighbours.get(column);
        return info.getQueries();
    }

    public Set<Column> getColumns ()
    {
        return this.neighbours.keySet();
    }

    public class NeighbourInfo
    {
        private Set<Query> queries = null;
        private int location = 0;
        private int length;

        public NeighbourInfo(int location, int length, Query query)
        {
            this.location = location;
            this.length = length;
            this.queries = new HashSet<>();
            this.queries.add(query);
        }

        public int getLength()
        {
            return length;
        }

        public void setLength(int size)
        {
            this.length = size;
        }

        public int getLocation()
        {
            return location;
        }

        public void setLocation(int location)
        {
            this.location = location;
        }

        public Set<Query> getQueries()
        {
            return queries;
        }

        public void addQuery(Query query)
        {
            this.queries.add(query);
        }
    }
}
