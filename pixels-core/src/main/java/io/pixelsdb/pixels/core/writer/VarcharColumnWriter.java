package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.ColumnVector;

/**
 * pixels
 *
 * @author guodong
 */
public class VarcharColumnWriter extends BaseColumnWriter
{
    public VarcharColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
    }

    @Override
    public int write(ColumnVector vector, int length)
    {
        return 0;
    }
}
