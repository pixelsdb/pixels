package cn.edu.ruc.iir.pixels.common.metadata.domain;

import java.util.ArrayList;
import java.util.List;

public class Splits
{
    private int numRowGroupInBlock;
    private List<SplitPattern> splitPatterns = new ArrayList<>();

    public int getNumRowGroupInBlock()
    {
        return numRowGroupInBlock;
    }

    public void setNumRowGroupInBlock(int numRowGroupInBlock)
    {
        this.numRowGroupInBlock = numRowGroupInBlock;
    }

    public List<SplitPattern> getSplitPatterns()
    {
        return splitPatterns;
    }

    public void setSplitPatterns(List<SplitPattern> splitPatterns)
    {
        this.splitPatterns = splitPatterns;
    }

    public void addSplitPatterns(SplitPattern splitPattern)
    {
        this.splitPatterns.add(splitPattern);
    }
}
