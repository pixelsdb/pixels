package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class FloatColumnWriter extends BaseColumnWriter
{
    public FloatColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
    }

    @Override
    public int write(ColumnVector vector, int length) throws IOException
    {
        DoubleColumnVector columnVector = (DoubleColumnVector) vector;
        double[] values = columnVector.vector;
        ByteBuffer buffer = ByteBuffer.allocate(length * Double.BYTES);
        for (int i = 0; i < length; i++)
        {
            curPixelEleCount++;
            float value = (float) values[i];
            buffer.putFloat(value);
            curPixelPosition += Float.BYTES;
            pixelStatRecorder.updateFloat(value);
            // if current pixel size satisfies the pixel stride, end the current pixel and start a new one
            if (curPixelEleCount >= pixelStride) {
                newPixel();
            }
        }
        // append buffer of this batch to rowBatchBufferList
        buffer.flip();
//        rowBatchBufferList.add(buffer);
//        colChunkSize += buffer.limit();
        return buffer.limit();
    }

    @Override
    public void newPixel() throws IOException
    {}
}
