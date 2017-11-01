package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord;

import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.LogFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.Algorithm;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import org.apache.commons.logging.Log;

import java.util.*;

/**
 * Created by hank on 2015/04/27
 * Updated by hank on 2016/10/12 and 2017/4/5 to use the new Algorithm interface.
 * The simulated annealing column order algorithm.
 */
public class Scoa extends Algorithm
{
    protected Random rand = new Random(System.nanoTime());
    protected double coolingRate = 0.003;
    protected volatile double temperature = 100000;
    protected volatile long iterations = 0;
    protected Log log = LogFactory.Instance().getLog();
    protected double currentEnergy = 0;

    // currently this candidate queue is not used, we do not control the sequence of
    // neighbours to be accepted when there are multiple concurrent threads running this algorithm.
    private List<NeighbourState> candidateQueue = new ArrayList<>();

    private class NeighbourState
    {
        private List<Column> columnOrder = null;
        private double energy = 0.0;

        public NeighbourState (List<Column> columnOrder, double energy)
        {
            this.columnOrder = columnOrder;
            this.energy = energy;
        }

        public List<Column> ColumnOrder()
        {
            return columnOrder;
        }

        public double Energy()
        {
            return energy;
        }
    }

    public Scoa () {}

    protected void setTemperature (double temperature)
    {
        this.temperature = temperature;
    }

    protected void setCoolingRate (double coolingRate)
    {
        this.coolingRate = coolingRate;
    }

    protected double getTemperature()
    {
        return temperature *= (1 - coolingRate);
    }

    @Override
    public boolean isMultiThreaded()
    {
        return true;
    }

    public long getIterations()
    {
        return iterations;
    }

    @Override
    public void setup()
    {
        super.setColumnOrder(super.getSchema());
        String strCoolingRate = ConfigFactory.Instance().getProperty("scoa.cooling_rate");
        String strInitTemp = ConfigFactory.Instance().getProperty("scoa.init.temperature");
        if (strCoolingRate != null)
        {
            this.coolingRate = Double.parseDouble(strCoolingRate);
        }
        if (strInitTemp != null)
        {
            this.temperature = Double.parseDouble(strInitTemp);
        }
    }

    @Override
    public void cleanup()
    {
        log.info("total iterations: " + this.iterations + "");
    }

    /**
     * get the neighbour of the current column order
     * @return
     */
    protected List<Column> getNeighbour()
    {

        super.getColumnOrderLock().lock();

        List<Column> neighbour;
        try
        {
            neighbour = new ArrayList<Column>(super.getColumnOrder());
        } finally
        {
            super.getColumnOrderLock().unlock();
        }

        int i = rand.nextInt(neighbour.size());

        int j = i;
        while (j == i)
        {
            j = rand.nextInt(neighbour.size());
        }

        Column c = neighbour.get(i);
        neighbour.set(i, neighbour.get(j));
        neighbour.set(j, c);
        rand.setSeed(System.nanoTime());
        return neighbour;
    }

    /**
     * decide if accept the neighbour.
     * @param neighbour
     * @param neighbourEnergy
     * @return
     */
    protected boolean accept(List<Column> neighbour, double neighbourEnergy)
    {
        super.getColumnOrderLock().lock();
        try
        {
            double temperature = this.getTemperature();
            if (this.probability(currentEnergy, neighbourEnergy, temperature) > Math.random())
            {
                List<Column> old = super.getColumnOrder();
                super.setColumnOrder(neighbour);
                old.clear();
                currentEnergy = neighbourEnergy;
                return true;
            }
            return false;

        } finally
        {
            super.getColumnOrderLock().unlock();
        }
   }

    protected double probability(double e, double e1, double temperature)
    {
        if (e1 < e)
        {
            return 1;
        } else
        {
            return Math.exp((e - e1) / temperature);
        }
    }

    /**
     * This function is faster than calculating the seek cost of a neighbour column ordering from getWorkloadSeekCoost
     * @param neighbor
     * @return
     */
    protected double getNeighbourSeekCost(List<Column> neighbor)
    {
        // new a comparator, this is the way in java7. in java8, we can use Comparator.<Integer>naturalOrder() instead.
        Comparator<Integer> naturalOrder = new Comparator<Integer>()
        {
            @Override
            public int compare(Integer o1, Integer o2)
            {
                return o1 - o2;
            }
        };

        //make a prefix summation array
        List<Double> sum = new ArrayList<Double>();

        for (int i = 0; i < neighbor.size(); i++)
        {
            if (i == 0)
            {
                sum.add(0.0);
            }
            else
            {
                sum.add(sum.get(i - 1) + neighbor.get(i - 1).getSize());
            }
        }

        //mark a map from column id to neighbor index
        Map<Integer, Integer> idMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < neighbor.size(); i++)
        {
            idMap.put(neighbor.get(i).getId(), i);
        }

        //calculate cost
        double workloadSeekCost = 0;
        for (Query query : super.getWorkload())
        {
            // curColumns is the index of each column in the neighbour ordering.
            List<Integer> curColumns = new ArrayList<>(query.getColumnIds());
            for (int i = 0; i < curColumns.size(); i++)
            {
                curColumns.set(i, idMap.get(curColumns.get(i)));
            }
            curColumns.sort(naturalOrder);

            double queryCost = 0;
            double lastPos = 0;
            for (int i = 0; i < curColumns.size(); i++)
            {
                // id is the index of the column in neighbour
                int id = curColumns.get(i);
                if (lastPos != 0)
                {
                    queryCost += this.getSeekCostFunction().calculate(sum.get(id) - lastPos);
                }
                lastPos = sum.get(id) + neighbor.get(id).getSize();
            }
            workloadSeekCost += query.getWeight() * queryCost;
        }
        return workloadSeekCost;
    }

    @Override
    public void runAlgorithm()
    {
        long startSeconds = System.currentTimeMillis() / 1000;
        this.currentEnergy = super.getCurrentWorkloadSeekCost();
        double neighbourEnergy = 0;

        //System.out.println("Initial energy of scoa is : " + this.currentEnergy);
        //System.out.println("SCOA budget : "  + super.getComputationBudget());

        for (long currentSeconds = System.currentTimeMillis() / 1000;
             (currentSeconds - startSeconds) < super.getComputationBudget();
             currentSeconds = System.currentTimeMillis() / 1000, ++this.iterations)
        {
            List<Column> neighbour = this.getNeighbour();
            neighbourEnergy = this.getNeighbourSeekCost(neighbour);

            this.accept(neighbour, neighbourEnergy);
        }
    }

}
