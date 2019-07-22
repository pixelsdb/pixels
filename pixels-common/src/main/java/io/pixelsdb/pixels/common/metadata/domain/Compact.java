package io.pixelsdb.pixels.common.metadata.domain;

import java.util.ArrayList;
import java.util.List;

public class Compact
{
    private int numRowGroupInBlock;
    private int numColumn;
    private int cacheBorder;
    private List<String> columnletOrder = new ArrayList<>();

    public int getNumRowGroupInBlock()
    {
        return numRowGroupInBlock;
    }

    public void setNumRowGroupInBlock(int numRowGroupInBlock)
    {
        this.numRowGroupInBlock = numRowGroupInBlock;
    }

    public int getNumColumn()
    {
        return numColumn;
    }

    public void setNumColumn(int numColumn)
    {
        this.numColumn = numColumn;
    }

    public int getCacheBorder()
    {
        return cacheBorder;
    }

    public void setCacheBorder(int cacheBorder)
    {
        this.cacheBorder = cacheBorder;
    }

    public List<String> getColumnletOrder()
    {
        return columnletOrder;
    }

    public void setColumnletOrder(List<String> columnletOrder)
    {
        this.columnletOrder = columnletOrder;
    }

    public void addColumnletOrder(String columnlet)
    {
        this.columnletOrder.add(columnlet);
    }
}
