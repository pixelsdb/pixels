package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

/**
 * pixels
 *
 * @author guodong
 */
public abstract class ColumnWriter
{
    private final int id = 0;
    private final TypeDescription schema;

    public ColumnWriter(TypeDescription schema)
    {
        this.schema = schema;
    }

    public abstract void writeBatch(ColumnVector vector, int offset, int length);
}
