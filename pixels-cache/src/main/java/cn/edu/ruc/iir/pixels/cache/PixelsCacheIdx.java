package cn.edu.ruc.iir.pixels.cache;

import java.nio.ByteBuffer;
import java.util.Objects;

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

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(offset).append(", ");
        sb.append(timestamp).append(", ");
        sb.append(length).append(", ");
        sb.append(counter);
        return sb.toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (other != null && other instanceof PixelsCacheIdx) {
            PixelsCacheIdx o = (PixelsCacheIdx) other;
            return Objects.equals(offset, o.offset) &&
                    Objects.equals(timestamp, o.timestamp) &&
                    Objects.equals(length, o.length) &&
                    Objects.equals(counter, o.counter);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(offset, timestamp, length, counter);
    }
}
