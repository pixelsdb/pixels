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
public class IntegerColumnWriter extends BaseColumnWriter
{
    public IntegerColumnWriter(TypeDescription schema, int pixelStride)
    {
        super(schema, pixelStride);
    }

    @Override
    public int writeBatch(ColumnVector vector)
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        long[] values = columnVector.vector;
        ByteBuffer buffer = ByteBuffer.allocate(values.length * Long.BYTES);
        for (int i = 0; i < values.length; i++)
        {
            curPixelSize++;
            long value = values[i];
            buffer.putLong(value);
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
        return colChunkSize;
    }

    @Override
    public byte[] serializeContent()
    {
        ByteBuffer tempBuffer = ByteBuffer.allocate(colChunkSize);
        for (ByteBuffer buffer: rowBatchBufferList)
        {
            tempBuffer.put(buffer);
        }
        return tempBuffer.array();
    }
}
