package cn.edu.ruc.iir.pixels.core.compactor;

public class ColumnletInfo
{
    private int columnId = -1;
    private int rowGroupId = -1;

    public ColumnletInfo(int rowGroupId, int columnId)
    {
        this.rowGroupId = rowGroupId;
        this.columnId = columnId;
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
