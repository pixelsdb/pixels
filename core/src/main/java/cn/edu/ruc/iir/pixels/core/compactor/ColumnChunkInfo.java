package cn.edu.ruc.iir.pixels.core.compactor;

public class ColumnChunkInfo
{
    private int columnId = -1;
    private int rowGroupId = -1;

    public ColumnChunkInfo(int columnId, int rowGroupId)
    {
        this.columnId = columnId;
        this.rowGroupId = rowGroupId;
    }

    public int getColumnId()
    {
        return columnId;
    }

    public int getRowGroupId()
    {
        return rowGroupId;
    }
}
