package cn.edu.ruc.iir.rainbow.layout.domian;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by hank on 17-1-10.
 */
public class WorkloadPattern
{
    private Map<Query, Set<Column>> queryColumnSetMap = null;
    private double seekCost = 0;

    public WorkloadPattern()
    {
        this.queryColumnSetMap = new HashMap<>();
    }

    public double getSeekCost()
    {
        return this.seekCost;
    }

    public void setSeekCost (double seekCost)
    {
        this.seekCost = seekCost;
    }

    public void increaseSeekCost (double seekCost)
    {
        this.seekCost += seekCost;
    }

    public void reduceSeekCost (double seekCost)
    {
        this.seekCost -= seekCost;
    }

    public boolean existPattern (Query query)
    {
        return this.queryColumnSetMap.containsKey(query);
    }

    public void setPattern (Query query, Set<Column> columnSet)
    {
        this.queryColumnSetMap.put(query, columnSet);
    }

    public Map<Query, Set<Column>> getPatterns ()
    {
        return this.queryColumnSetMap;
    }

    public Set<Query> getQuerySet ()
    {
        return this.queryColumnSetMap.keySet();
    }

    /**
     * find the accessed column set of the query
     * @param query
     * @return
     */
    public Set<Column> getColumnSet (Query query)
    {
        return this.queryColumnSetMap.get(query);
    }

    @Override
    public WorkloadPattern clone()
    {
        WorkloadPattern clonedPattern = new WorkloadPattern();
        for (Map.Entry<Query, Set<Column>> entry : this.queryColumnSetMap.entrySet())
        {
            clonedPattern.setPattern(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        clonedPattern.seekCost = this.seekCost;

        return clonedPattern;
    }
}
