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
// todo varchar column writer. basically the same as string column writer
public class VarcharColumnWriter extends BaseColumnWriter
{
    private final int maxLength;

    public VarcharColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
        maxLength = schema.getMaxLength();
    }

    @Override
    public int write(ColumnVector vector, int length) throws IOException
    {
        BytesColumnVector columnVector = (BytesColumnVector) vector;
        byte[][] values = columnVector.vector;
        int size = 0;
        for (byte[] v : values) {
            size += Math.min(v.length, maxLength);
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (int i = 0; i < length; i++) {
            curPixelEleCount++;
            byte[] v = values[i];
            int itemLength = Math.min(v.length, maxLength);
            buffer.put(v, 0, itemLength);
            curPixelPosition += itemLength;
            pixelStatRecorder.updateString(new String(v, 0, itemLength, Charset.forName("UTF-8")), 1);
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
}
