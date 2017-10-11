package cn.edu.ruc.iir.rainbow.layout.domian;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by hank on 2015/9/11.
 */
public class QueryCluster
{
    private Set<Query> queries = null;
    private Set<Column> columns = null;
    private int hashCode = -1;

    public QueryCluster ()
    {
        this.queries = new HashSet<>();
        this.columns = new HashSet<>();
    }

    @Override
    public int hashCode()
    {
        if (this.hashCode >= 0)
        {
            return this.hashCode;
        }
        else
        {
            int hc = 0;
            for (Query query : this.queries)
            {
                hc += query.hashCode();
            }
            this.hashCode = hc;
            return hc;
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        return super.equals(obj);
    }

    public boolean equalsColumns (Object obj)
    {
        if (obj instanceof QueryCluster)
        {
            QueryCluster cluster = (QueryCluster) obj;
            for (Column column : this.columns)
            {
                if (!cluster.columns.contains(column))
                {
                    return false;
                }
            }
            if (this.columns.size() == cluster.columns.size())
            {
                return true;
            }
        }
        else if (obj instanceof Query)
        {
            Set<Integer> cids = new HashSet<>();
            for (Column column : this.columns)
            {
                cids.add(column.getId());
            }
            Query query = (Query)obj;
            for (int cid : query.getColumnIds())
            {
                if (!cids.contains(cid))
                {
                    return false;
                }
            }
            if (cids.size() == query.getColumnIds().size())
            {
                return true;
            }
        }
        return false;
    }

    public Set<Query> getQueries()
    {
        return queries;
    }

    public void addQueries(Collection<Query> queries)
    {
        this.queries.addAll(queries);
    }

    public Set<Column> getColumns()
    {
        return columns;
    }

    public void addQuery(Query query)
    {
        this.queries.add(query);
    }

    public void addColumns(Collection<Column> columns)
    {
        this.columns.addAll(columns);
    }
}
