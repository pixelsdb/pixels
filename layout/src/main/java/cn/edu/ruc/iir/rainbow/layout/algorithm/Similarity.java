package cn.edu.ruc.iir.rainbow.layout.algorithm;

import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;

import java.util.Map;
import java.util.Set;

/**
 * Created by hank on 2015/9/11.
 */
public abstract class Similarity
{
    private Map<Query, Set<Column>> queryToColumns = null;
    double[][] simMatrix = null;

    public void setQueryToColumns (Map<Query, Set<Column>> queryToColumns)
    {
        this.queryToColumns = queryToColumns;
        simMatrix = new double[this.queryToColumns.size()][this.queryToColumns.size()];
        for (Query query1 : this.queryToColumns.keySet())
        {
            for (Query query2 : this.queryToColumns.keySet())
            {
                simMatrix[query1.getId()][query2.getId()] = this.calculate(query1, query2);
            }
        }
    }

    public abstract double calculate (Query q1, Query q2);

    public double calculate (Set<Query> qs1, Set<Query> qs2)
    {
        double avgSim = 0;
        for (Query q1 : qs1)
        {
            for(Query q2 : qs2)
            {
                double sim = this.simMatrix[q1.getId()][q2.getId()];
                avgSim += sim;
            }
        }
        return avgSim / qs1.size() / qs2.size();
    }
}
