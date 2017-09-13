package cn.edu.ruc.iir.pixels.core.compactor;

import java.util.ArrayList;
import java.util.List;

public class CompactLayout
{
    private int rowGroupNumber = 0;
    private int columnNumber = 0;
    private List<ColumnChunkInfo> layout = null;

    public CompactLayout(int rowGroupNumber, int columnNumber)
    {
        this.rowGroupNumber = rowGroupNumber;
        this.columnNumber = columnNumber;
        this.layout = new ArrayList<>(rowGroupNumber*columnNumber);
    }

    public void addColumnChunk (int columnId, int rowGroupId)
    {
        this.layout.add(new ColumnChunkInfo(columnId, rowGroupId));
    }

    public int size()
    {
        return rowGroupNumber * columnNumber;
    }

    public ColumnChunkInfo get (int i)
    {
        return this.layout.get(i);
    }
}
