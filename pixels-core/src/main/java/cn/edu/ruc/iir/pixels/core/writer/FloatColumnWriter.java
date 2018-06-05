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
    public int write(ColumnVector vector, int length) throws IOException
    {
        DoubleColumnVector columnVector = (DoubleColumnVector) vector;
        double[] values = columnVector.vector;
        boolean[] isNull = new boolean[pixelStride];
        int isNullIndex = 0;
        for (int i = 0; i < length; i++)
        {
            isNull[isNullIndex++] = columnVector.isNull[i];
            curPixelEleCount++;
            float value = (float) values[i];
            encodingUtils.writeFloat(outputStream, value);
            pixelStatRecorder.updateFloat(value);
            // if current pixel size satisfies the pixel stride, end the current pixel and start a new one
            if (curPixelEleCount >= pixelStride) {
                newPixel(isNull);
                isNullIndex = 0;
            }
        }
        return outputStream.size();
    }
}
