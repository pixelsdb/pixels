package cn.edu.ruc.iir.pixels.core.compactor;

import java.util.ArrayList;
import java.util.List;

public class CompactLayout
{
    private int rowGroupNumber = 0;
    private int columnNumber = 0;
    private List<ColumnletInfo> layout = null;

    public CompactLayout(int rowGroupNumber, int columnNumber)
    {
        this.rowGroupNumber = rowGroupNumber;
        this.columnNumber = columnNumber;
        this.layout = new ArrayList<>(rowGroupNumber*columnNumber);
    }

    public void addColumnlet (int rowGroupId, int columnId)
    {
        this.layout.add(new ColumnletInfo(rowGroupId, columnId));
    }

    public int size()
    {
        return rowGroupNumber * columnNumber;
    }

    public ColumnletInfo get (int i)
    {
        return this.layout.get(i);
    }
}
