package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenByteEncoder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;

import java.io.IOException;

/**
 * pixels byte column writer
 *
 * @author guodong
 */
public class ByteColumnWriter extends BaseColumnWriter
{
    private final byte[] curPixelVector = new byte[pixelStride];

    public ByteColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
        encoder = new RunLenByteEncoder();
    }

    @Override
    public int write(ColumnVector vector, int size) throws IOException
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        long[] values = columnVector.vector;
        byte[] bvalues = new byte[size];
        for (int i = 0; i < size; i++)
        {
            bvalues[i] = (byte) values[i];
        }
        int curPartLength;
        int curPartOffset = 0;
        int nextPartLength = size;

        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride) {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartByte(columnVector, bvalues, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurPartByte(columnVector, bvalues, curPartLength, curPartOffset);

        return outputStream.size();
    }

    private void writeCurPartByte(LongColumnVector columnVector, byte[] bvalues, int curPartLength, int curPartOffset)
    {
        for (int i = 0; i < curPartLength; i++) {
            if (columnVector.isNull[i + curPartOffset])
            {
                hasNull = true;
            }
            else
            {
                curPixelVector[curPixelEleIndex] = bvalues[i + curPartOffset];
                curPixelEleIndex++;
            }
        }
        System.arraycopy(columnVector.isNull, curPartOffset, isNull, curPixelIsNullIndex, curPartLength);
        curPixelIsNullIndex += curPartLength;
    }

    @Override
    public void newPixel() throws IOException
    {
        for (int i = 0; i < curPixelEleIndex; i++)
        {
            pixelStatRecorder.updateInteger(curPixelVector[i], 1);
        }

        if (isEncoding) {
            outputStream.write(encoder.encode(curPixelVector, 0, curPixelEleIndex));
        }
        else {
            outputStream.write(curPixelVector, 0, curPixelEleIndex);
        }

        super.newPixel();
    }

    @Override
    public PixelsProto.ColumnEncoding.Builder getColumnChunkEncoding()
    {
        if (isEncoding) {
            return PixelsProto.ColumnEncoding.newBuilder()
                    .setKind(PixelsProto.ColumnEncoding.Kind.RUNLENGTH);
        }
        return PixelsProto.ColumnEncoding.newBuilder()
                .setKind(PixelsProto.ColumnEncoding.Kind.NONE);
    }

    @Override
    public void close() throws IOException
    {
        encoder.close();
        super.close();
    }
}
