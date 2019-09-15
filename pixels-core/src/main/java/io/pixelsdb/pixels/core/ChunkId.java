package io.pixelsdb.pixels.core;

/**
 * pixels
 *
 * @author guodong
 */
public class ChunkId
{
    private final int rowGroupId;
    private final int columnId;
    private final long offset;
    private final long length;

    public ChunkId(int rowGroupId, int columnId, long offset, long length)
    {
        this.rowGroupId = rowGroupId;
        this.columnId = columnId;
        this.offset = offset;
        this.length = length;
    }

    public int getRowGroupId()
    {
        return rowGroupId;
    }

    public int getColumnId()
    {
        return columnId;
    }

    public long getOffset()
    {
        return offset;
    }

    public long getLength()
    {
        return length;
    }
}
