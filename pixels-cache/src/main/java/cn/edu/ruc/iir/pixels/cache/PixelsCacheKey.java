package cn.edu.ruc.iir.pixels.cache;

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
    private static final int SIZE = Long.BYTES + 2 * Short.BYTES;
    private static final ByteBuffer keyBuffer = ByteBuffer.allocate(SIZE);
    private long blockId;
    private short rowGroupId;
    private short columnId;

    PixelsCacheKey(long blockId, short rowGroupId, short columnId)
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

    public byte[] getBytes()
    {
        keyBuffer.clear();
        keyBuffer.putLong(blockId);
        keyBuffer.putShort(rowGroupId);
        keyBuffer.putShort(columnId);
        return keyBuffer.array();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
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
