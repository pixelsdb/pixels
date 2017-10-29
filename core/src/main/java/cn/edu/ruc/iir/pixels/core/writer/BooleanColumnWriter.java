package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class BooleanColumnWriter extends BaseColumnWriter
{
    public BooleanColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
    }

    @Override
    public int write(ColumnVector vector, int length) throws IOException
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        long[] values = columnVector.vector;
        ByteBuffer buffer = ByteBuffer.allocate(length * Integer.BYTES);
        for (int i = 0; i < length; i++)
        {
            curPixelEleCount++;
            int value =(int) values[i];
            buffer.putInt(value == 0 ? 0 : 1);
            curPixelPosition += Integer.BYTES;
            pixelStatRecorder.updateBoolean(value != 0, 1);
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
