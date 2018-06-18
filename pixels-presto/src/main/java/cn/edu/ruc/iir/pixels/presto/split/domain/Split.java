package cn.edu.ruc.iir.pixels.presto.split.domain;

import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.domain.split
 * @ClassName: Split
 * @Description:
 * @author: tao
 * @date: Create in 2018-02-25 10:05
 **/
public class Split {
    private int numRowGroupInBlock;
    private List<SplitPattern> splitPatterns;

    public Split() {
    }

    public Split(int numRowGroupInBlock, List<SplitPattern> splitPatterns) {
        this.numRowGroupInBlock = numRowGroupInBlock;
        this.splitPatterns = splitPatterns;
    }

    public int getNumRowGroupInBlock() {
        return numRowGroupInBlock;
    }

    public void setNumRowGroupInBlock(int numRowGroupInBlock) {
        this.numRowGroupInBlock = numRowGroupInBlock;
    }

    public List<SplitPattern> getSplitPatterns() {
        return splitPatterns;
    }

    public void setSplitPatterns(List<SplitPattern> splitPatterns) {
        this.splitPatterns = splitPatterns;
    }
}
