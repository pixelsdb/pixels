package cn.edu.ruc.iir.rainbow.layout.domian;

/**
 * Created by hank on 2015/9/15.
 */
public class ClusterPair implements Comparable<ClusterPair>
{
    private QueryCluster cluster1 = null;
    private QueryCluster cluster2 = null;
    private double similarity = 0;

    public ClusterPair(QueryCluster cluster1, QueryCluster cluster2, double similarity)
    {
        this.cluster1 = cluster1;
        this.cluster2 = cluster2;
        this.similarity = similarity;
    }

    public QueryCluster getCluster1()
    {
        return cluster1;
    }

    public void setCluster1(QueryCluster cluster1)
    {
        this.cluster1 = cluster1;
    }

    public QueryCluster getCluster2()
    {
        return cluster2;
    }

    public void setCluster2(QueryCluster cluster2)
    {
        this.cluster2 = cluster2;
    }

    public double getSimilarity()
    {
        return similarity;
    }

    public void setSimilarity(double similarity)
    {
        this.similarity = similarity;
    }

    @Override
    public int compareTo(ClusterPair c)
    {
        if (c instanceof ClusterPair)
        {
            if (this.similarity > c.similarity)
            {
                return -1;
            }
            else if (this.similarity < c.similarity)
            {
                return 1;
            }
        }
        return 0;
    }
}
