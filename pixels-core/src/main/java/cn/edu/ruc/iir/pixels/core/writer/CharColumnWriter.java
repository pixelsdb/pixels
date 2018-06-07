package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

/**
 * pixels column writer for <code>Char</code>
 *
 * @author guodong
 */
// todo char column writer. basically the same as string column writer.
public class CharColumnWriter extends BaseColumnWriter
{
    public CharColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
    }

    @Override
    public int write(ColumnVector vector, int length)
    {
        return 0;
    }
}
