package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.cache.mq.MappedBusMessage;
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
public class ColumnletId
        implements MappedBusMessage
{
    private static final int SIZE = Long.BYTES + 2 * Short.BYTES + Constants.MAX_BLOCK_ID_LEN;
    private static final ByteBuffer keyBuffer = ByteBuffer.allocate(SIZE);

    private String blockId;
    private short rowGroupId;
    private short columnId;
    long cacheOffset;
    int cacheLength;

    public ColumnletId(String blockId, short rowGroupId, short columnId)
    {
        this.blockId = blockId;
        this.rowGroupId = rowGroupId;
        this.columnId = columnId;
    }

    public ColumnletId()
    {}

    public short getColumnId()
    {
        return columnId;
    }

    public short getRowGroupId()
    {
        return rowGroupId;
    }

    public byte[] getBytes()
    {
        keyBuffer.clear();
        keyBuffer.put(blockId.getBytes(Charset.forName(CharsetNames.UTF_8)));
        keyBuffer.putShort(rowGroupId);
        keyBuffer.putShort(columnId);
        return Arrays.copyOfRange(keyBuffer.array(), 0, keyBuffer.position());
    }

    /**
     * Writes a message to the bus.
     *
     * @param mem an instance of the memory mapped file
     * @param pos the start of the current record
     */
    @Override
    public void write(MemoryMappedFile mem, long pos)
    {
//        mem.putLong(0, blockId);
//        mem.putShort(8, rowGroupId);
//        mem.putShort(12, columnId);
    }

    /**
     * Reads a message from the bus.
     *
     * @param mem an instance of the memory mapped file
     * @param pos the start of the current record
     */
    @Override
    public void read(MemoryMappedFile mem, long pos)
    {
        mem.getLong(0);
        mem.getShort(8);
        mem.getShort(12);
    }

    /**
     * Returns the message type.
     *
     * @return the message type
     */
    @Override
    public int type()
    {
        return 0;
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
        ColumnletId other = (ColumnletId) o;
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
