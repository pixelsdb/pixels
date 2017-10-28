package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;

import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class LongColumnWriter extends BaseColumnWriter
{
    public LongColumnWriter(TypeDescription schema, int pixelStride)
    {
        super(schema, pixelStride);
    }

    @Override
    public int writeBatch(ColumnVector vector, int length, boolean encoding)
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        long[] values = columnVector.vector;
        ByteBuffer buffer = ByteBuffer.allocate(length * Long.BYTES);
        for (int i = 0; i < length; i++)
        {
            curPixelSize++;
            long value = values[i];
            buffer.putLong(value);
            curPixelPosition += Long.BYTES;
            pixelStatRecorder.updateInteger(value, 1);
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
