/*
 * Copyright 2021 PixelsDB.
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
import io.pixelsdb.pixels.core.vector.DateColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Date column writer.
 * All date values are converted to the number of days from
 * UTC 1970-1-1 0:0：0 before they are stored as int values.
 *
 * 2021-04-25
 * @author hank
 */
public class DateColumnWriter extends BaseColumnWriter
{
    private final int[] curPixelVector = new int[pixelStride];

    public DateColumnWriter(TypeDescription type, int pixelStride, EncodingLevel encodingLevel, ByteOrder byteOrder)
    {
        super(type, pixelStride, encodingLevel, byteOrder);
        // Issue #94: Date.getTime() can be negative if the date is before 1970-1-1.
        encoder = new RunLenIntEncoder(true, true);
    }

    @Override
    public int write(ColumnVector vector, int size) throws IOException
    {
        DateColumnVector columnVector = (DateColumnVector) vector;
        int[] dates = columnVector.dates;
        int curPartLength;
        int curPartOffset = 0;
        int nextPartLength = size;

        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
        {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartTime(columnVector, dates, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurPartTime(columnVector, dates, curPartLength, curPartOffset);

        return outputStream.size();
    }

    private void writeCurPartTime(DateColumnVector columnVector, int[] values, int curPartLength, int curPartOffset)
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
    public void newPixel() throws IOException
    {
        for (int i = 0; i < curPixelVectorIndex; i++)
        {
            pixelStatRecorder.updateDate(curPixelVector[i]);
        }

        if (encodingLevel.ge(EncodingLevel.EL1))
        {
            int[] values = new int[curPixelVectorIndex];
            System.arraycopy(curPixelVector, 0, values, 0, curPixelVectorIndex);
            outputStream.write(encoder.encode(values));
        }
        else
        {
            ByteBuffer curVecPartitionBuffer =
                    ByteBuffer.allocate(curPixelVectorIndex * Integer.BYTES);
            curVecPartitionBuffer.order(byteOrder);
            for (int i = 0; i < curPixelVectorIndex; i++)
            {
                curVecPartitionBuffer.putInt(curPixelVector[i]);
            }
            outputStream.write(curVecPartitionBuffer.array());
        }

        super.newPixel();
    }

    @Override
    public PixelsProto.ColumnEncoding.Builder getColumnChunkEncoding()
    {
        if (encodingLevel.ge(EncodingLevel.EL1))
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
