package cn.edu.ruc.iir.rainbow.layout.algorithm;

import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.seekcost.SeekCostFunction;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * author: v-habia (haoqiong bian)
 * date: 2015/04/27
 * updated 2016/10/12: user schema to store the original column order and add reentrant lock to control the current column order
 * updated 2017/4/5: add isMultiThreaded function
 * The subclass of Algorithm must have a non-parameter constructor.
 */
public abstract class Algorithm
{
    // computation budget may not be used by some algorithm
    private long computationBudget;
    // schema is the initial column order
    private List<Column> schema;
    private List<Column> currentColumnOrder;
    private List<Query> workload;
    private SeekCostFunction seekCostFunction;

    private Lock columnOrderLock = new ReentrantLock();

    protected Algorithm()
    {
    }

    public long getComputationBudget()
    {
        return this.computationBudget;
    }

    protected void setComputationBudget(long computationBudget)
    {
        this.computationBudget = computationBudget;
    }

    protected List<Column> getSchema ()
    {
        return this.schema;
    }

    protected void setSchema(List<Column> schema)
    {
        this.schema = schema;
    }

    public SeekCostFunction getSeekCostFunction()
    {
        return seekCostFunction;
    }

    protected void setSeekCostFunction(SeekCostFunction seekCostFunction)
    {
        this.seekCostFunction = seekCostFunction;
    }

    protected List<Query> getWorkload()
    {
        return this.workload;
    }

    protected void setWorkload(List<Query> workload)
    {
        this.workload = workload;
    }

    // set and update the current column order
    protected void setColumnOrder(List<Column> columnOrder)
    {
        this.currentColumnOrder = columnOrder;
    }

    // get the current column order
    public List<Column> getColumnOrder()
    {
        return currentColumnOrder;
    }

    /**
     * in the subclass of this class, any method uses or changes the column order, must call this method to get the ReentrantLock.
     * @return
     */
    protected synchronized Lock getColumnOrderLock()
    {
        return this.columnOrderLock;
    }

    public abstract boolean isMultiThreaded ();

    /**
     * setup configurations for the algorithm.
     */
    public void setup()
    {
    }

    public abstract void runAlgorithm();

    /**
     * release resources and collect results.
     */
    public void cleanup()
    {
    }

    /**
     * get the seek cost of a query (on the given column order).
     * this is a general function, sub classes can override it.
     * @param columnOrder
     * @param query
     * @return
     */
    protected double getQuerySeekCost(List<Column> columnOrder, Query query)
    {
        double querySeekCost = 0, seekDistance = 0;
        int accessedColumnNum = 0;
        for (int i = columnOrder.size() - 1; i >= 0; --i)
        {
            if (query.getColumnIds().contains(columnOrder.get(i).getId()))
            {
                // column i has been accessed by the query
                querySeekCost += this.seekCostFunction.calculate(seekDistance);
                seekDistance = 0;
                ++accessedColumnNum;
                if (accessedColumnNum >= query.getColumnIds().size())
                {
                    // the query has accessed all the necessary columns
                    break;
                }
            } else
            {
                // column i has been skipped (seek over) by the query
                seekDistance += columnOrder.get(i).getSize();
            }
        }
        return querySeekCost;
    }

    /**
     * get the seek cost of the whole workload (on the current column order).
     * @return
     */
    public double getCurrentWorkloadSeekCost()
    {
        double workloadSeekCost = 0;
        List<Column> columnOrder = this.getColumnOrder();
        for (Query query : this.getWorkload())
        {
            workloadSeekCost += query.getWeight() * getQuerySeekCost(columnOrder, query);
        }

        return workloadSeekCost;
    }

    /**
     * get the seek cost of the whole workload (on the given column order).
     * @return
     */
    public double getWorkloadSeekCost(List<Query> workload, List<Column> columnOrder)
    {
        double workloadSeekCost = 0;

        for (Query query : workload)
        {
            workloadSeekCost += query.getWeight() * getQuerySeekCost(columnOrder, query);
        }

        return workloadSeekCost;
    }

    public double getSchemaSeekCost()
    {
        double workloadSeekCost = 0;
        this.columnOrderLock.lock();
        try
        {
            List<Column> columnOrder = this.schema;
            for (Query query : this.workload)
            {
                workloadSeekCost += query.getWeight() * getQuerySeekCost(columnOrder, query);
            }
        } finally
        {
            this.columnOrderLock.unlock();
        }

        return workloadSeekCost;
    }
}
