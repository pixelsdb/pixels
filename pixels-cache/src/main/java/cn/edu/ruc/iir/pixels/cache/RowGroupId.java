package cn.edu.ruc.iir.pixels.cache;

/**
 * pixels
 *
 * @author guodong
 */
public class RowGroupId
{
    private final long blockId;
    private final short rgId;

    public RowGroupId(long blockId, short rgId)
    {
        this.blockId = blockId;
        this.rgId = rgId;
    }

    public long getBlockId()
    {
        return blockId;
    }

    public short getRgId()
    {
        return rgId;
    }
}
