package io.pixelsdb.pixels.common.metadata.domain;

import java.util.ArrayList;
import java.util.List;

public class SplitPattern
{
    private List<Integer> accessedColumns = new ArrayList<>();
    private int numRowGroupInSplit;

    public List<Integer> getAccessedColumns()
    {
        return accessedColumns;
    }

    public void setAccessedColumns(List<Integer> accessedColumns)
    {
        this.accessedColumns = accessedColumns;
    }


    public void addAccessedColumns(int accessedColumn)
    {
        this.accessedColumns.add(accessedColumn);
    }

    public int getNumRowGroupInSplit()
    {
        return numRowGroupInSplit;
    }

    public void setNumRowGroupInSplit(int numRowGroupInSplit)
    {
        this.numRowGroupInSplit = numRowGroupInSplit;
    }
}
