package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.utils.BitUtils;
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
    private final boolean[] isNull = new boolean[pixelStride];

    public DoubleColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
        encodingUtils = new EncodingUtils();
    }

    @Override
    public int write(ColumnVector vector, int length) throws IOException
    {
        DoubleColumnVector columnVector = (DoubleColumnVector) vector;
        double[] values = columnVector.vector;
        int isNullIndex = 0;
        for (int i = 0; i < length; i++)
        {
            isNull[isNullIndex++] = vector.isNull[i];
            curPixelEleCount++;
            double value = values[i];
            encodingUtils.writeLongLE(outputStream, Double.doubleToLongBits(value));
            pixelStatRecorder.updateDouble(value);
            // if current pixel size satisfies the pixel stride, end the current pixel and start a new one
            if (curPixelEleCount >= pixelStride) {
                newPixel();
                isNullIndex = 0;
            }
        }
        return outputStream.size();
    }

    @Override
    public void newPixel() throws IOException
    {
        isNullStream.write(BitUtils.bitWiseCompact(isNull, curPixelEleCount));

        super.newPixel();
    }
}
