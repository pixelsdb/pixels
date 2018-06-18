package cn.edu.ruc.iir.pixels.presto.split.domain;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.domain.split
 * @ClassName: SplitPattern
 * @Description:
 * @author: tao
 * @date: Create in 2018-02-25 10:06
 **/
public class SplitPattern {
    private String accessedColumns;
    private int numRowGroupInSplit;

    public SplitPattern() {
    }

    public SplitPattern(String accessedColumns, int numRowGroupInSplit) {
        this.accessedColumns = accessedColumns;
        this.numRowGroupInSplit = numRowGroupInSplit;
    }

    public String getAccessedColumns() {
        return accessedColumns;
    }

    public void setAccessedColumns(String accessedColumns) {
        this.accessedColumns = accessedColumns;
    }

    public int getNumRowGroupInSplit() {
        return numRowGroupInSplit;
    }

    public void setNumRowGroupInSplit(int numRowGroupInSplit) {
        this.numRowGroupInSplit = numRowGroupInSplit;
    }
}
