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
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.encoding.RunLenIntEncoder;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.IntColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The column writer for integers.
 *
 * @author guodong, hank
 * @update 2023-08-16 Chamonix: support nulls padding
 */
public class IntegerColumnWriter extends BaseColumnWriter
{
    private final long[] curPixelVector = new long[pixelStride];        // current pixel value vector haven't written out yet
    private final boolean isLong;                                       // current column type is long or int, used for the first pixel
    private final boolean runlengthEncoding;
    private int typeDescriptionMode;
    public IntegerColumnWriter(TypeDescription type,  PixelsWriterOption writerOption)
    {
        super(type, writerOption);
        isLong = type.getCategory() == TypeDescription.Category.LONG;
        runlengthEncoding = encodingLevel.ge(EncodingLevel.EL2);
        if (runlengthEncoding)
        {
            encoder = new RunLenIntEncoder();
        }
    }

    @Override
    public int write(ColumnVector vector, int size) throws IOException
    {
        if(vector instanceof LongColumnVector)
        {
            LongColumnVector longColumnVector = (LongColumnVector) vector;
            typeDescriptionMode = TypeDescription.Mode.NONE;
            writeVector(vector, size, (i, offset) -> longColumnVector.vector[i + offset]);
        } else if (vector instanceof IntColumnVector)
        {
            IntColumnVector intColumnVector = (IntColumnVector) vector;
            writeVector(vector, size, (i, offset) -> (long) intColumnVector.vector[i + offset]);
        }  else
        {
            throw new IllegalArgumentException("Unsupported ColumnVector type: " + vector.getClass().getName());
        }

        return outputStream.size();
    }

    @FunctionalInterface
    private interface ValueAccessor {
        long get(int i, int offset);
    }

    private void writeVector(ColumnVector columnVector, int size, ValueAccessor accessor) throws IOException
    {
        int curPartLength;           // size of the partition which belongs to current pixel
        int curPartOffset = 0;       // starting offset of the partition which belongs to current pixel
        int nextPartLength = size;   // size of the partition which belongs to next pixel

        // do the calculation to partition the vector into current pixel and next one
        // doing this pre-calculation to eliminate branch prediction inside the for loop
        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
        {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartLong(columnVector, accessor, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurPartLong(columnVector, accessor, curPartLength, curPartOffset);
    }

    private void writeCurPartLong(ColumnVector columnVector, ValueAccessor accessor, int curPartLength, int curPartOffset)
    {
        for (int i = 0; i < curPartLength; i++)
        {
            curPixelEleIndex++;
            if (columnVector.isNull[i + curPartOffset])
            {
                hasNull = true;
                pixelStatRecorder.increment();
                if (nullsPadding)
                {
                    // padding 0 for nulls
                    curPixelVector[curPixelVectorIndex++] = 0L;
                }
            }
            else
            {
                curPixelVector[curPixelVectorIndex++] = accessor.get(i, curPartOffset);
            }
        }
        System.arraycopy(columnVector.isNull, curPartOffset, isNull, curPixelIsNullIndex, curPartLength);
        curPixelIsNullIndex += curPartLength;
    }

    @Override
    void newPixel() throws IOException
    {
        // write out current pixel vector
        if (runlengthEncoding)
        {
            for (int i = 0; i < curPixelVectorIndex; i++)
            {
                pixelStatRecorder.updateInteger(curPixelVector[i], 1);
            }
            outputStream.write(encoder.encode(curPixelVector, 0, curPixelVectorIndex));
        }
        else
        {
            ByteBuffer curVecPartitionBuffer;
            if (isLong)
            {
                curVecPartitionBuffer = ByteBuffer.allocate(curPixelVectorIndex * Long.BYTES);
                curVecPartitionBuffer.order(byteOrder);
                for (int i = 0; i < curPixelVectorIndex; i++)
                {
                    curVecPartitionBuffer.putLong(curPixelVector[i]);
                    pixelStatRecorder.updateInteger(curPixelVector[i], 1);
                }
            }
            else
            {
                curVecPartitionBuffer = ByteBuffer.allocate(curPixelVectorIndex * Integer.BYTES);
                curVecPartitionBuffer.order(byteOrder);
                for (int i = 0; i < curPixelVectorIndex; i++)
                {
                    curVecPartitionBuffer.putInt((int) curPixelVector[i]);
                    pixelStatRecorder.updateInteger(curPixelVector[i], 1);
                }
            }
            outputStream.write(curVecPartitionBuffer.array());
        }

        super.newPixel();
    }

    @Override
    public PixelsProto.ColumnEncoding.Builder getColumnChunkEncoding()
    {
        if (runlengthEncoding)
        {
            return PixelsProto.ColumnEncoding.newBuilder()
                    .setKind(PixelsProto.ColumnEncoding.Kind.RUNLENGTH);
        }
        return PixelsProto.ColumnEncoding.newBuilder()
                .setKind(PixelsProto.ColumnEncoding.Kind.NONE);
    }

    @Override
    public void close() throws IOException
    {
        if (runlengthEncoding)
        {
            encoder.close();
        }
        super.close();
    }

    @Override
    public boolean decideNullsPadding(PixelsWriterOption writerOption)
    {
        if (writerOption.getEncodingLevel().ge(EncodingLevel.EL2))
        {
            return false;
        }
        return writerOption.isNullsPadding();
    }
}
