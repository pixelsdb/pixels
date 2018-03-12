package cn.edu.ruc.iir.pixels.cache;

/**
 * pixels
 *
 * @author guodong
 */
public class ColumnletIdx
{
    private final long offset;
    private final int length;
    private final int count;

    public ColumnletIdx(long offset, int length, int count)
    {
        this.offset = offset;
        this.length = length;
        this.count = count;
    }

    public long getOffset()
    {
        return offset;
    }

    public int getLength()
    {
        return length;
    }

    public int getCount()
    {
        return count;
    }
}
