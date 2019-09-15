package io.pixelsdb.pixels.cache;

import java.nio.ByteBuffer;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCacheKey
{
    final static int SIZE = 2 * Short.BYTES + Long.BYTES;
    public long blockId;
    public short rowGroupId;
    public short columnId;
    // Big-endian is prefix comparable and efficient for radix-tree.
    // Although big endian is used as the default byte order in ByteBuffer, we still want to make sure.
    // Block id in hdfs-2.7.3 is a sequence number in each block-id pool, not really random.
    // Currently we only support HDFS cluster without NameNode federation, in which there is only one
    // block-id pool.

    public PixelsCacheKey(long blockId, short rowGroupId, short columnId)
    {
        this.blockId = blockId;
        this.rowGroupId = rowGroupId;
        this.columnId = columnId;
    }

    public void getBytes(ByteBuffer keyBuffer)
    {
        keyBuffer.clear();
        keyBuffer.putLong(blockId);
        keyBuffer.putShort(rowGroupId);
        keyBuffer.putShort(columnId);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        PixelsCacheKey other = (PixelsCacheKey) o;
        return Objects.equals(blockId, other.blockId) &&
                Objects.equals(rowGroupId, other.rowGroupId) &&
                Objects.equals(columnId, other.columnId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("block id", blockId)
                .add("row group id", rowGroupId)
                .add("column id", columnId)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(blockId, rowGroupId, columnId);
    }
}
