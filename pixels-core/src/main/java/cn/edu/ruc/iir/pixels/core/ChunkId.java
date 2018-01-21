package cn.edu.ruc.iir.pixels.core;

/**
 * pixels
 *
 * @author guodong
 */
public class ChunkId
{
    private final String path;
    private final int rowGroupId;
    private final long offset;
    private final long length;

    public ChunkId(String path, int rowGroupId, int offset, int length)
    {
        this.path = path;
        this.rowGroupId = rowGroupId;
        this.offset = offset;
        this.length = length;
    }

    public String getPath()
    {
        return path;
    }

    public int getRowGroupId()
    {
        return rowGroupId;
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
