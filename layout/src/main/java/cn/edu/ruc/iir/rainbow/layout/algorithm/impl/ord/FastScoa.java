package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord;

import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.seekcost.SeekCostFunction;

import java.util.*;

/**
 * Algorithm designed by Wenbo on 2015/10/30.
 * Created by Hank on 2017/1/9.
 * updated on 2017/4/10 to use the new Algorithm interface.
 * This class is not multi-threaded.
 */
public class FastScoa extends Scoa
{
    protected List<TreeSet<Integer>> queryAccessedPos = new ArrayList<>();

    @Override
    public boolean isMultiThreaded()
    {
        return false;
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
        //make initial columnId-columnIndex map
        Map<Integer, Integer> cidToCIdxMap = new HashMap<>();
        for (int i = 0; i < this.getColumnOrder().size(); i ++)
        {
            cidToCIdxMap.put(this.getColumnOrder().get(i).getId(), i);
        }

        //build initial querys' accessed column index sets.
        for (int i = 0; i < this.getWorkload().size(); i ++)
        {
            Query curQuery = this.getWorkload().get(i);
            queryAccessedPos.add(new TreeSet<Integer>());
            for (int colIds : curQuery.getColumnIds())
            {
                // add the column indexes to query i's tree set.
                queryAccessedPos.get(i).add(cidToCIdxMap.get(colIds));
            }
        }
    }

    @Override
    public void runAlgorithm()
    {
        long startSeconds = System.currentTimeMillis() / 1000;
        this.currentEnergy = super.getCurrentWorkloadSeekCost();

        //System.out.println("Initial energy of scoa is : " + this.currentEnergy);
        //System.out.println("SCOA budget : "  + super.getComputationBudget());

        //minCost = this.currentEnergy;
        //bestColumnOrder = this.getColumnOrder();

        for (long currentSeconds = System.currentTimeMillis() / 1000;
             (currentSeconds - startSeconds) < super.getComputationBudget();
             currentSeconds = System.currentTimeMillis() / 1000, ++this.iterations)
        {
            //generate two random indices
            int i = rand.nextInt(this.getColumnOrder().size());
            int j = i;
            while (j == i)
                j = rand.nextInt(this.getColumnOrder().size());
            rand.setSeed(System.nanoTime());

            //calculate new cost
            double neighbourEnergy = getNeighbourSeekCost(i, j);

            //try to accept it
            double temperature = this.getTemperature();
            if (this.probability(currentEnergy, neighbourEnergy, temperature) > Math.random())
            {
                currentEnergy = neighbourEnergy;
                updateColumnOrder(i, j);
            }
        }
        //System.out.println("Final energy of scoa is : " + currentEnergy);
        //this.setColumnOrder(bestColumnOrder);
    }

    /**
     * swap column x and y in the column order and
     * modify the accessed column index of the queries.
     * @param x the index of column x
     * @param y the index of column y
     */
    protected void updateColumnOrder(int x, int y)
    {
        //swap
        Column t = this.getColumnOrder().get(x);
        this.getColumnOrder().set(x, this.getColumnOrder().get(y));
        this.getColumnOrder().set(y, t);

        //update qid
        for (int i = 0; i < this.getWorkload().size(); i++)
        {
            TreeSet<Integer> curSet = queryAccessedPos.get(i);
            int tot = 0;
            if (curSet.contains(x)) tot++;
            if (curSet.contains(y)) tot++;
            if (tot != 1) continue;

            if (curSet.contains(x))
            {
                curSet.remove(x);
                curSet.add(y);
            } else
            {
                curSet.remove(y);
                curSet.add(x);
            }
        }
    }

    /**
     * return the previous element of index x in s
     * @param s
     * @param x
     * @return
     */
    protected int getPrev(TreeSet<Integer> s, int x)
    {
        SortedSet head = s.headSet(x);
        if (head.isEmpty()) return -1;
        return (int) head.last();
    }

    /**
     * return the successive element of index x in s
     * @param s
     * @param x
     * @return
     */
    protected int getSucc(TreeSet<Integer> s, int x)
    {
        SortedSet tail = s.tailSet(x);
        if (tail.isEmpty()) return -1;
        return (int) tail.first();
    }

    protected double getNeighbourSeekCost(int x, int y)
    {
        int C = this.getColumnOrder().size();
        int Q = this.getWorkload().size();
        SeekCostFunction sc = this.getSeekCostFunction();
        double []sb = new double[C];
        double []se = new double[C];
        for (int i = 0; i < C; i ++)
        {
            sb[i] = (i == 0 ? 0 : sb[i - 1] + this.getColumnOrder().get(i - 1).getSize());
            se[i] = (i == 0 ? this.getColumnOrder().get(0).getSize() :
                                se[i - 1] + this.getColumnOrder().get(i).getSize());
        }

        double sizeX = this.getColumnOrder().get(x).getSize();
        double sizeY = this.getColumnOrder().get(y).getSize();
        double originSeekCost = this.currentEnergy;


        double deltaCost = 0;
        for (int i = 0; i < Q; i ++)
        {
            double delta = 0;
            TreeSet<Integer> curSet = queryAccessedPos.get(i);
            if (! curSet.contains(x) && ! curSet.contains(y))
            {
                int prevX = getPrev(curSet, x); int succX = getSucc(curSet, x);
                int prevY = getPrev(curSet, y); int succY = getSucc(curSet, y);
                if (succX != succY)
                {
                    if (prevX >= 0 && succX >= 0)
                    {
                        delta -= sc.calculate(sb[succX] - se[prevX]);
                        delta += sc.calculate(sb[succX] - se[prevX]
                                                - sizeX
                                                + sizeY);
                    }
                    if (prevY >= 0 && succY >= 0)
                    {
                        delta -= sc.calculate(sb[succY] - se[prevY]);
                        delta += sc.calculate(sb[succY] - se[prevY]
                                                - sizeY
                                                + sizeX);
                    }
                }
            }
            else if (curSet.contains(x) && curSet.contains(y))
            {
                //The easiest case, do nothing!!!!
            }
            else
            {
                if (curSet.contains(y))
                {
                    //do swap
                    int t; t = x; x = y; y = t;
                }
                curSet.remove(x);
                int prevX = getPrev(curSet, x); int succX = getSucc(curSet, x);
                int prevY = getPrev(curSet, y); int succY = getSucc(curSet, y);
                if (succX == succY) //special case
                {
                    if (prevX >= 0)
                        delta -= sc.calculate(sb[x] - se[prevX]);
                    if (succX >= 0)
                        delta -= sc.calculate(sb[succX] - se[x]);
                    //add
                    if (x < y)
                    {
                        if (succX >= 0)
                            delta += sc.calculate(sb[succX] - se[y]);
                        if (prevX >= 0)
                            delta += sc.calculate(sb[y] - se[prevX]
                                                    - sizeX
                                                    + sizeY);
                    }
                    else
                    {
                        if (prevX >= 0)
                            delta += sc.calculate(sb[y] - se[prevX]);
                        if (succX >= 0)
                            delta += sc.calculate(sb[succX] - se[y]
                                                    - sizeX
                                                    + sizeY);
                    }
                }
                else
                {
                    //minus
                    if (prevX >= 0)
                        delta -= sc.calculate(sb[x] - se[prevX]);
                    if (succX >= 0)
                        delta -= sc.calculate(sb[succX] - se[x]);
                    if (prevY >= 0 && succY >= 0)
                        delta -= sc.calculate(sb[succY] - se[prevY]);
                    //add
                    if (prevY >= 0)
                        delta += sc.calculate(sb[y] - se[prevY]);
                    if (succY >= 0)
                        delta += sc.calculate(sb[succY] - se[y]);
                    if (prevX >= 0 && succX >= 0)
                        delta += sc.calculate(sb[succX] - se[prevX]
                                                - sizeX
                                                + sizeY);
                }
                curSet.add(x);
            }
            deltaCost += delta * this.getWorkload().get(i).getWeight();
        }
        return originSeekCost + deltaCost;
    }
}
