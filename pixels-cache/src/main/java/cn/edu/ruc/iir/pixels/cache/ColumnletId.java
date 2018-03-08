package cn.edu.ruc.iir.pixels.cache;

/**
 * pixels
 *
 * @author guodong
 */
public class ColumnletId
{
    private final long blockId;
    private final int rowGroupId;
    private final int columnId;

    public ColumnletId(long blockId, int rowGroupId, int columnId)
    {
        this.blockId = blockId;
        this.rowGroupId = rowGroupId;
        this.columnId = columnId;
    }

    public long getBlockId()
    {
        return blockId;
    }

    public int getRowGroupId()
    {
        return rowGroupId;
    }

    public int getColumnId()
    {
        return columnId;
    }
}
