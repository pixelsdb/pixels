package io.pixelsdb.pixels.common.metadata.domain;

import java.util.ArrayList;
import java.util.List;

public class Order
{
    private List<String> columnOrder = new ArrayList<>();

    public List<String> getColumnOrder()
    {
        return columnOrder;
    }

    public void setColumnOrder(List<String> columnOrder)
    {
        this.columnOrder = columnOrder;
    }

    public void addColumnOrder(String column)
    {
        this.columnOrder.add(column);
    }
}
