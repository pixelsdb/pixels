package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.BytesColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
// todo char column writer. basically the same as string column writer.
public class CharColumnWriter extends BaseColumnWriter
{
    public CharColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
    }

    @Override
    public int write(ColumnVector vector, int length) throws IOException
    {
        BytesColumnVector columnVector = (BytesColumnVector) vector;
        byte[][] values = columnVector.vector;
        ByteBuffer buffer = ByteBuffer.allocate(length);
        for (int i = 0; i < length; i++) {
            curPixelEleCount++;
            byte[] v = values[i];
            assert v.length == 1;
            char vc = (char) v[0];
            buffer.putChar(vc);
            curPixelPosition++;
            pixelStatRecorder.updateString(String.valueOf(vc), 1);
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
