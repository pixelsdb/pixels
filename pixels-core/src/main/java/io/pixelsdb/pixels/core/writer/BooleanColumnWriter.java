package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.BitUtils;
import io.pixelsdb.pixels.core.vector.ByteColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;

import java.io.IOException;

/**
 * Boolean column writer.
 * Boolean values are compacted using bit-wise bytes, and then these integers are written out.
 *
 * @author guodong
 */
public class BooleanColumnWriter extends BaseColumnWriter
{
    private final byte[] curPixelVector = new byte[pixelStride];

    public BooleanColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
    }

    @Override
    public int write(ColumnVector vector, int size)
            throws IOException
    {
        ByteColumnVector columnVector = (ByteColumnVector) vector;
        byte[] values = columnVector.vector;
        int curPartLength;
        int curPartOffset = 0;
        int nextPartLength = size;

        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
        {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurBoolean(columnVector, values, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurBoolean(columnVector, values, curPartLength, curPartOffset);

        return outputStream.size();
    }

    private void writeCurBoolean(ByteColumnVector columnVector, byte[] values, int curPartLength, int curPartOffset)
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
                curPixelVector[curPixelVectorIndex++] = values[i + curPartOffset];
            }
        }
        System.arraycopy(columnVector.isNull, curPartOffset, isNull, curPixelIsNullIndex, curPartLength);
        curPixelIsNullIndex += curPartLength;
    }

    @Override
    public void newPixel()
            throws IOException
    {
        for (int i = 0; i < curPixelVectorIndex; i++)
        {
            pixelStatRecorder.updateBoolean(curPixelVector[i] != 0, 1);
        }

        outputStream.write(BitUtils.bitWiseCompact(curPixelVector, curPixelVectorIndex));

        super.newPixel();
    }
}
