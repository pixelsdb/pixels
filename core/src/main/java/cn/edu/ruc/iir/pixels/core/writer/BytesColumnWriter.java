package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

/**
 * pixels
 *
 * @author guodong
 */
public class BytesColumnWriter extends BaseColumnWriter
{
    public BytesColumnWriter(TypeDescription schema, int pixelStride)
    {
        super(schema, pixelStride);
    }

    @Override
    public int writeBatch(ColumnVector vector)
    {
        return 0;
    }

    @Override
    public byte[] serializeContent()
    {
        return new byte[0];
    }
}
