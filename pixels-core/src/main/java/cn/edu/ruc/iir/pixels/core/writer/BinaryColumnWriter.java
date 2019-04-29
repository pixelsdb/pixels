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
    public int write(ColumnVector vector, int size)
            throws IOException
    {
        BytesColumnVector columnVector = (BytesColumnVector) vector;
        byte[][] values = columnVector.vector;
        int curPartLength;
        int curPartOffset = 0;
        int nextPartLength = size;

        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
        {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartBinary(columnVector, values, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurPartBinary(columnVector, values, curPartLength, curPartOffset);

        return outputStream.size();
    }

    private void writeCurPartBinary(BytesColumnVector columnVector, byte[][] values, int curPartLength, int curPartOffset)
            throws IOException
    {
        for (int i = 0; i < curPartLength; i++)
        {
            curPixelEleIndex++;
            if (columnVector.isNull[i + curPartOffset])
            {
                hasNull = true;
                pixelStatRecorder.increment();
            }
            else
            {
                byte[] bytes = values[curPartOffset + i];
                outputStream.write(bytes.length);
                outputStream.write(bytes);
                pixelStatRecorder.updateBinary(bytes, 0, bytes.length, 1);
            }
        }
        System.arraycopy(columnVector.isNull, curPartOffset, isNull, curPixelIsNullIndex, curPartLength);
        curPixelIsNullIndex += curPartLength;
    }
}
