package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.utils.EncodingUtils;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public class FloatColumnWriter extends BaseColumnWriter
{
    private final EncodingUtils encodingUtils;

    public FloatColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
        encodingUtils = new EncodingUtils();
    }

    @Override
    public int write(ColumnVector vector, int length)
            throws IOException
    {
        DoubleColumnVector columnVector = (DoubleColumnVector) vector;
        long[] values = columnVector.vector;
        for (int i = 0; i < length; i++)
        {
            isNull[curPixelIsNullIndex++] = columnVector.isNull[i];
            curPixelEleIndex++;
            if (columnVector.isNull[i])
            {
                hasNull = true;
                pixelStatRecorder.increment();
            }
            else
            {
                int v = (int) values[i];
                encodingUtils.writeIntLE(outputStream, v);
                pixelStatRecorder.updateFloat(Float.intBitsToFloat(v));
            }
            // if current pixel size satisfies the pixel stride, end the current pixel and start a new one
            if (curPixelEleIndex >= pixelStride)
            {
                newPixel();
            }
        }
        return outputStream.size();
    }
}
