package cn.edu.ruc.iir.pixels.cache;

import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCacheIdx
{
    public static final int SIZE = 2 * Long.BYTES + 2 * Integer.BYTES;
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

    public byte[] getBytes()
    {
        ByteBuffer buffer = ByteBuffer.allocate(SIZE);
        buffer.putLong(offset);
        buffer.putLong(timestamp);
        buffer.putInt(length);
        buffer.putInt(counter);

        return buffer.array();
    }
}
