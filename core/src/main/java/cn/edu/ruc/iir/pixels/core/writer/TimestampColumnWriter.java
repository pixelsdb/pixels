package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.TimestampColumnVector;

import java.nio.ByteBuffer;
import java.sql.Timestamp;

/**
 * pixels
 *
 * @author guodong
 */
public class TimestampColumnWriter extends BaseColumnWriter
{
    public TimestampColumnWriter(TypeDescription schema, int pixelStride)
    {
        super(schema, pixelStride);
    }

    @Override
    public int writeBatch(ColumnVector vector, int length, boolean encoding)
    {
        TimestampColumnVector columnVector = (TimestampColumnVector) vector;
        ByteBuffer buffer = ByteBuffer.allocate(length * Long.BYTES);
        for (int i = 0; i < length; i++) {
            curPixelSize++;
            Timestamp timestamp = columnVector.asScratchTimestamp(i);
            buffer.putLong(timestamp.getTime());
            curPixelPosition += Long.BYTES;
            pixelStatRecorder.updateTimestamp(timestamp);
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
