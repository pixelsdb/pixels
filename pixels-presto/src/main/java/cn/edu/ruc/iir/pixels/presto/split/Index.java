package cn.edu.ruc.iir.pixels.presto.split;


import cn.edu.ruc.iir.pixels.presto.split.domain.AccessPattern;
import cn.edu.ruc.iir.pixels.presto.split.domain.ColumnSet;

public interface Index {
    /**
     * search viable access pattern for a column set
     *
     * @param columnSet
     * @return
     */
    public AccessPattern search(ColumnSet columnSet);
}
