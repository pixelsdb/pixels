package cn.edu.ruc.iir.pixels.presto.split.domain;

import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.domain.split
 * @ClassName: SplitPattern
 * @Description:
 * @author: tao
 * @date: Create in 2018-02-25 10:06
 **/
public class SplitPattern {
    private List<String> accessedColumns;
    private int numRowGroupInSplit;

    public SplitPattern() {
    }

    public SplitPattern(List<String> accessedColumns, int numRowGroupInSplit) {
        this.accessedColumns = accessedColumns;
        this.numRowGroupInSplit = numRowGroupInSplit;
    }

    public List<String> getAccessedColumns() {
        return accessedColumns;
    }

    public void setAccessedColumns(List<String> accessedColumns) {
        this.accessedColumns = accessedColumns;
    }

    public int getNumRowGroupInSplit() {
        return numRowGroupInSplit;
    }

    public void setNumRowGroupInSplit(int numRowGroupInSplit) {
        this.numRowGroupInSplit = numRowGroupInSplit;
    }
}
