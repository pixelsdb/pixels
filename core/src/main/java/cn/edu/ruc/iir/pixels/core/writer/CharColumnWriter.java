package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.BytesColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class CharColumnWriter extends BaseColumnWriter
{
    public CharColumnWriter(TypeDescription schema, int pixelStride)
    {
        super(schema, pixelStride);
    }

    @Override
    public int writeBatch(ColumnVector vector, int length, boolean encoding)
    {
        BytesColumnVector columnVector = (BytesColumnVector) vector;
        byte[][] values = columnVector.vector;
        ByteBuffer buffer = ByteBuffer.allocate(length);
        for (int i = 0; i < length; i++) {
            curPixelSize++;
            byte[] v = values[i];
            assert v.length == 1;
            char vc = (char) v[0];
            buffer.putChar(vc);
            curPixelPosition++;
            pixelStatRecorder.updateString(String.valueOf(vc), 1);
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
