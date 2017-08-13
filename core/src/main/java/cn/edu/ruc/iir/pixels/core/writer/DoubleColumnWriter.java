package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

/**
 * pixels
 *
 * @author guodong
 */
public class DoubleColumnWriter extends BaseColumnWriter
{
    public DoubleColumnWriter(TypeDescription schema, int pixelStride)
    {
        super(schema, pixelStride);
    }

    @Override
    public void writeBatch(ColumnVector vector)
    {

    }

    @Override
    public byte[] serializeContent()
    {
        return new byte[0];
    }
}
