package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.TypeDescription;

/**
 * pixels
 *
 * @author guodong
 */
public class ByteColumnReader
        extends ColumnReader
{
    public ByteColumnReader(TypeDescription type)
    {
        super(type);
    }

    @Override
    public byte[] readBytes(byte[] bytes, boolean isEncoding, int num)
    {
        return new byte[0];
    }
}
