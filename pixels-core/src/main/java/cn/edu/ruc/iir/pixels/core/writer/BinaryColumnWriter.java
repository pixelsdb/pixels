package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.BytesColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

import java.io.IOException;

/**
 * pixels binary column writer.
 * each element consists of content length and content binary.
 *
 * @author guodong
 */
public class BinaryColumnWriter extends BaseColumnWriter
{
    public BinaryColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
    }

    @Override
    public int write(ColumnVector vector, int size) throws IOException
    {
        BytesColumnVector columnVector = (BytesColumnVector) vector;
        byte[][] values = columnVector.vector;
        int curPartLength;
        int curPartOffset = 0;
        int nextPartLength = size;

        while ((curPixelEleCount + nextPartLength) >= pixelStride) {
            curPartLength = pixelStride - curPixelEleCount;
            for (int i = 0; i < curPartLength; i++) {
                byte[] bytes = values[curPartOffset + i];
                outputStream.write(bytes.length);
                outputStream.write(bytes);
                pixelStatRecorder.updateBinary(bytes, 0, bytes.length, 1);
            }
            curPixelEleCount += curPartLength;
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        for (int i = 0; i < curPartLength; i++) {
            byte[] bytes = values[curPartOffset + i];
            outputStream.write(bytes.length);
            outputStream.write(bytes);
            pixelStatRecorder.updateBinary(bytes, 0, bytes.length, 1);
        }
        curPixelEleCount += curPartLength;
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;

        if (nextPartLength > 0) {
            curPartLength = nextPartLength;
            for (int i = 0; i < curPartLength; i++) {
                byte[] bytes = values[curPartOffset + i];
                outputStream.write(bytes.length);
                outputStream.write(bytes);
                pixelStatRecorder.updateBinary(bytes, 0, bytes.length, 1);
            }
            curPixelEleCount += nextPartLength;
        }
        return outputStream.size();
    }
}
