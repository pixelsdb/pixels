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

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Column writer for BINARY and VARBINARY.
 * Each non-null value is stored as a 4-byte little/big-endian length followed by its payload.
 * Null values write no content bytes. Values longer than the type max length are truncated.
 *
 * @author guodong, hank, gengdy
 * @update 2023-08-16 Chamonix: support nulls padding
 * @update 2026-08-05: use 4-byte length prefix, honor vector start/lens, and disable nulls padding
 */
public class BinaryColumnWriter extends BaseColumnWriter
{
    /**
     * Max length of binary. It is recorded in the file footer's schema.
     */
    private final int maxLength;
    private int numTruncated;

    public BinaryColumnWriter(TypeDescription type,  PixelsWriterOption writerOption)
    {
        super(type, writerOption);
        this.maxLength = type.getMaxLength();
        this.numTruncated = 0;
    }

    @Override
    public int write(ColumnVector vector, int size) throws IOException
    {
        BinaryColumnVector columnVector = (BinaryColumnVector) vector;
        int curPartLength;
        int curPartOffset = 0;
        int nextPartLength = size;

        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
        {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartBinary(columnVector, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurPartBinary(columnVector, curPartLength, curPartOffset);

        return outputStream.size();
    }

    private void writeCurPartBinary(BinaryColumnVector columnVector, int curPartLength, int curPartOffset)
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
                int index = curPartOffset + i;
                byte[] bytes = columnVector.vector[index];
                int start = columnVector.start[index];
                int length = Math.min(columnVector.lens[index], maxLength);
                writeLength(length);
                outputStream.write(bytes, start, length);
                if (columnVector.lens[index] > maxLength)
                {
                    numTruncated++;
                }
                pixelStatRecorder.updateBinary(bytes, start, length, 1);
            }
        }
        System.arraycopy(columnVector.isNull, curPartOffset, isNull, curPixelIsNullIndex, curPartLength);
        curPixelIsNullIndex += curPartLength;
    }

    @Override
    public boolean decideNullsPadding(PixelsWriterOption writerOption)
    {
        return false;
    }

    @Override
    public void reset()
    {
        super.reset();
        this.numTruncated = 0;
    }

    /**
     * Get the number of values truncated to the type's maximum length.
     */
    public int getNumTruncated()
    {
        return this.numTruncated;
    }

    private void writeLength(int length)
    {
        if (byteOrder.equals(ByteOrder.LITTLE_ENDIAN))
        {
            outputStream.write(length);
            outputStream.write(length >>> 8);
            outputStream.write(length >>> 16);
            outputStream.write(length >>> 24);
        }
        else
        {
            outputStream.write(length >>> 24);
            outputStream.write(length >>> 16);
            outputStream.write(length >>> 8);
            outputStream.write(length);
        }
    }
}
