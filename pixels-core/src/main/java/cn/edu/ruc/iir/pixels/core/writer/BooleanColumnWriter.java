package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.utils.BitUtils;

import java.io.IOException;

/**
 * Boolean column writer.
 * Boolean values are compacted using bit-wise bytes, and then these integers are written out.
 *
 * @author guodong
 */
public class BooleanColumnWriter extends IntegerColumnWriter
{
    private final long[] curPixelVector = new long[pixelStride];

    public BooleanColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
    }

    @Override
    public void newPixel() throws IOException
    {
        for (int i = 0; i < curPixelEleIndex; i++)
        {
            pixelStatRecorder.updateBoolean(curPixelVector[i] != 0, 1);
        }

        outputStream.write(BitUtils.bitWiseCompact(curPixelVector, curPixelEleIndex));

        super.newPixel();
    }
}
