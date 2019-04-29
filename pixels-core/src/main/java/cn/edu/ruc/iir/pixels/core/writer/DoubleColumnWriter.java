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
public class DoubleColumnWriter extends BaseColumnWriter
{
    private final EncodingUtils encodingUtils;

    public DoubleColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
        encodingUtils = new EncodingUtils();
    }

    @Override
    public int write(ColumnVector vector, int length)
            throws IOException
    {
        DoubleColumnVector columnVector = (DoubleColumnVector) vector;
        double[] values = columnVector.vector;
        for (int i = 0; i < length; i++)
        {
            isNull[curPixelIsNullIndex++] = vector.isNull[i];
            curPixelEleIndex++;
            if (vector.isNull[i])
            {
                hasNull = true;
                pixelStatRecorder.increment();
            }
            else
            {
                double value = values[i];
                encodingUtils.writeLongLE(outputStream, Double.doubleToLongBits(value));
                pixelStatRecorder.updateDouble(value);
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
