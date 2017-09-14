package cn.edu.ruc.iir.rainbow.layout.algorithm;

import java.util.List;
import java.util.TreeSet;

/**
 * Created by hank on 17-4-7.
 *
 * This is the supper class for duplication algorithm
 */
public abstract class RefineAlgorithm extends Algorithm
{
    public abstract void setQueryAccessedPos(List<TreeSet<Integer>> queryAccessedPos);
}
