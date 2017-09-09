package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;

import java.nio.ByteBuffer;

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
    public int writeBatch(ColumnVector vector, int length)
    {
        DoubleColumnVector columnVector = (DoubleColumnVector) vector;
        double[] values = columnVector.vector;
        ByteBuffer buffer = ByteBuffer.allocate(length * Double.BYTES);
        for (int i = 0; i < length; i++)
        {
            curPixelSize++;
            double value = values[i];
            buffer.putDouble(value);
            curPixelPosition += Double.BYTES;
            pixelStatRecorder.updateDouble(value);
            // if current pixel size satisfies the pixel stride, end the current pixel and start a new one
            if (curPixelSize >= pixelStride) {
                newPixel();
            }
        }
        // append buffer of this batch to rowBatchBufferList
        buffer.flip();
        rowBatchBufferList.add(buffer);
        colChunkSize += buffer.limit();
        return buffer.limit();
    }
}
