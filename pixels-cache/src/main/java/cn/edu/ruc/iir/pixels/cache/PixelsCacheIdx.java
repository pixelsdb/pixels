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
    public static final int SIZE = Long.BYTES + Integer.BYTES;
    private static ByteBuffer buffer = ByteBuffer.allocate(SIZE);
    private final long offset;
    private final int length;

    public PixelsCacheIdx(long offset, int length)
    {
        this.offset = offset;
        this.length = length;
    }

    public long getOffset()
    {
        return offset;
    }

    public int getLength()
    {
        return length;
    }

    public byte[] getBytes()
    {
        buffer.clear();
        buffer.putLong(offset);
        buffer.putInt(length);

        return buffer.array();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(offset).append(", ").append(length);
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
                    Objects.equals(length, o.length);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(offset, length);
    }
}
