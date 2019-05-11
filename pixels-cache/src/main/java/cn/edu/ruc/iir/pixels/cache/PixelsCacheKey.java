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
    public final static int SIZE = 2 * Short.BYTES + Constants.MAX_BLOCK_ID_LEN;
    private String blockId;  // TODO: it could be better to use long as the type of blockid
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

    public void getBytes(ByteBuffer keyBuffer)
    {
        keyBuffer.clear();
        // TODO: is it better to ensure big-endian in keyBuffer? big-endian is prefix comparable,
        // from which radix-tree may benifit.
        // And we'd better use long (int64) for block id, instead of a string file name.
        // Fixed key length (12 bytes) should be more efficient. I noticed that block ids in hdfs-2.7.3 looks
        // like a sequence number, not really random.
        keyBuffer.put(blockId.getBytes(Charset.forName(CharsetNames.UTF_8)));
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
