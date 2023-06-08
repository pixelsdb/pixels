/*
 * Copyright 2017-2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.RunLenIntEncoder;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
    private final boolean isLong;                                       // current column type is long or int     // if the current pixel is the first pixel
    public IntegerColumnWriter(TypeDescription type, int pixelStride, boolean isEncoding)
    {
        super(type, pixelStride, isEncoding);
        encoder = new RunLenIntEncoder();
        this.isLong = type.getCategory() == TypeDescription.Category.LONG;
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

    private void writeCurPartLong(ColumnVector columnVector, long[] values, int curPartLength, int curPartOffset)
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
                curVecPartitionBuffer = ByteBuffer.allocate(curPixelVectorIndex * Long.BYTES);
                curVecPartitionBuffer.order(ByteOrder.LITTLE_ENDIAN);
                for (int i = 0; i < curPixelVectorIndex; i++)
                {
                    curVecPartitionBuffer.putLong(curPixelVector[i]);
                }
            }
            else
            {
                curVecPartitionBuffer = ByteBuffer.allocate(curPixelVectorIndex * Integer.BYTES);
                curVecPartitionBuffer.order(ByteOrder.LITTLE_ENDIAN);
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
