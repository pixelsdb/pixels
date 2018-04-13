package cn.edu.ruc.iir.pixels.cache;

import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCacheKey
{
    private static final int SIZE = Long.BYTES + 2 * Short.BYTES;
    private static final ByteBuffer keyBuffer = ByteBuffer.allocate(SIZE);
    private final long blockId;
    private final short rowGroupId;
    private final short columnId;

    public PixelsCacheKey(long blockId, short rowGroupId, short columnId)
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
//
        return keyBuffer.array();
    }
}
