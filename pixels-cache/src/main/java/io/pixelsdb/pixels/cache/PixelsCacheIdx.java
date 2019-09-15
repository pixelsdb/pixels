package io.pixelsdb.pixels.cache;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCacheIdx
{
    static final int SIZE = Long.BYTES + Integer.BYTES;
    public final long offset;
    public final int length;

    public int dramAccessCount;
    public int radixLevel;

    public PixelsCacheIdx(long offset, int length)
    {
        this.offset = offset;
        this.length = length;
    }

    public PixelsCacheIdx(byte[] content)
    {
        ByteBuffer idxBuffer = ByteBuffer.wrap(content);
        this.offset = idxBuffer.getLong();
        this.length = idxBuffer.getInt();
    }

    public void getBytes(ByteBuffer cacheIdxBuffer)
    {
        cacheIdxBuffer.clear();
        cacheIdxBuffer.putLong(offset);
        cacheIdxBuffer.putInt(length);
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
        if (other == this)
        {
            return true;
        }
        if (other != null && other instanceof PixelsCacheIdx)
        {
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
