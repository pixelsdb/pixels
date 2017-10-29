package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.BytesColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * pixels
 *
 * @author guodong
 */
public class StringColumnWriter extends BaseColumnWriter
{
    public StringColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
    }

    @Override
    public int write(ColumnVector vector, int length) throws IOException
    {
        BytesColumnVector columnVector = (BytesColumnVector) vector;
        byte[][] values = columnVector.vector;
        int size = 0;
        for (byte[] v : values) {
            size += v.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (int i = 0; i < length; i++) {
            curPixelEleCount++;
            byte[] v = values[i];
            buffer.put(v);
            curPixelPosition += v.length;
            // todo currently only support urf-8
            pixelStatRecorder.updateString(new String(v, Charset.forName("UTF-8")), 1);
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
