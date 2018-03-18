package cn.edu.ruc.iir.pixels.cache;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCacheIdx
{
    private final long offset;
    private final long timestamp;
    private final int length;
    private final int counter;

    public PixelsCacheIdx(long offset, long timestamp, int length, int content)
    {
        this.offset = offset;
        this.timestamp = timestamp;
        this.length = length;
        this.counter = content;
    }

    public long getOffset()
    {
        return offset;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public int getLength()
    {
        return length;
    }

    public int getCounter()
    {
        return counter;
    }
}
