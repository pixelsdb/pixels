package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.RunLenIntEncoder;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Integer column writer.
 * If encoding, use RunLength;
 * Else isLong(1 byte) + content
 *
 * @author guodong
 */
public class IntegerColumnWriter extends BaseColumnWriter
{
    private final long[] curPixelVector = new long[pixelStride];        // current pixel value vector haven't written out yet
    private final boolean isLong;                                       // current column type is long or int

    public IntegerColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
        encoder = new RunLenIntEncoder();
        this.isLong = schema.getCategory() == TypeDescription.Category.LONG;
    }

    @Override
    public int write(ColumnVector vector, int size)
            throws IOException
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        long[] values = columnVector.vector;
        int curPartLength;           // size of the partition which belongs to current pixel
        int curPartOffset = 0;       // starting offset of the partition which belongs to current pixel
        int nextPartLength = size;   // size of the partition which belongs to next pixel

        // do the calculation to partition the vector into current pixel and next one
        // doing this pre-calculation to eliminate branch prediction inside the for loop
        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
        {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartLong(columnVector, values, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurPartLong(columnVector, values, curPartLength, curPartOffset);

        return outputStream.size();
    }

    private void writeCurPartLong(LongColumnVector columnVector, long[] values, int curPartLength, int curPartOffset)
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
    void newPixel()
            throws IOException
    {
        // update stats
        for (int i = 0; i < curPixelVectorIndex; i++)
        {
            pixelStatRecorder.updateInteger(curPixelVector[i], 1);
        }

        // write out current pixel vector
        if (isEncoding)
        {
            outputStream.write(encoder.encode(curPixelVector, 0, curPixelVectorIndex));
        }
        else
        {
            ByteBuffer curVecPartitionBuffer;
            if (isLong)
            {
                curVecPartitionBuffer = ByteBuffer.allocate(curPixelVectorIndex * Long.BYTES + 1);
                curVecPartitionBuffer.put((byte) 1);
                for (int i = 0; i < curPixelVectorIndex; i++)
                {
                    curVecPartitionBuffer.putLong(curPixelVector[i]);
                }
            }
            else
            {
                curVecPartitionBuffer = ByteBuffer.allocate(curPixelVectorIndex * Integer.BYTES + 1);
                curVecPartitionBuffer.put((byte) 0);
                for (int i = 0; i < curPixelVectorIndex; i++)
                {
                    curVecPartitionBuffer.putInt((int) curPixelVector[i]);
                }
            }
            outputStream.write(curVecPartitionBuffer.array());
        }

        super.newPixel();
    }

    @Override
    public PixelsProto.ColumnEncoding.Builder getColumnChunkEncoding()
    {
        if (isEncoding)
        {
            return PixelsProto.ColumnEncoding.newBuilder()
                    .setKind(PixelsProto.ColumnEncoding.Kind.RUNLENGTH);
        }
        return PixelsProto.ColumnEncoding.newBuilder()
                .setKind(PixelsProto.ColumnEncoding.Kind.NONE);
    }

    @Override
    public void close()
            throws IOException
    {
        encoder.close();
        super.close();
    }
}
