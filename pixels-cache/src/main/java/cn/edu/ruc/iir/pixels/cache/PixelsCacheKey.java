package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.common.utils.Constants;
import org.apache.commons.compress.utils.CharsetNames;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsCacheKey
{
    private final int SIZE = 2 * Short.BYTES + Constants.MAX_BLOCK_ID_LEN;
    private final ByteBuffer keyBuffer = ByteBuffer.allocate(SIZE);
    private String blockId;
    private short rowGroupId;
    private short columnId;

    public PixelsCacheKey(String blockId, short rowGroupId, short columnId)
    {
        this.blockId = blockId;
        this.rowGroupId = rowGroupId;
        this.columnId = columnId;
    }

    public String getBlockId()
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
        keyBuffer.put(blockId.getBytes(Charset.forName(CharsetNames.UTF_8)));
        keyBuffer.putShort(rowGroupId);
        keyBuffer.putShort(columnId);
        return Arrays.copyOfRange(keyBuffer.array(), 0, keyBuffer.position());
    }

    public int getSize()
    {
        return keyBuffer.position();
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
