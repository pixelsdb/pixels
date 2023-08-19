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
import io.pixelsdb.pixels.core.utils.EncodingUtils;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DateColumnVector;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Date column writer.
 * All date values are converted to the number of days from
 * UTC 1970-1-1 0:0：0 before they are stored as int values.
 *
 * @author hank
 * @create 2021-04-25
 * @update 2023-08-16 Chamonix: support nulls padding
 * @update 2023-08-19 Zermatt: reduce memory copy and fix curPixelsVector encoding when the current pixel contains nulls
 */
public class DateColumnWriter extends BaseColumnWriter
{
    private final int[] curPixelVector = new int[pixelStride];
    private final EncodingUtils encodingUtils = new EncodingUtils();
    private final boolean runlengthEncoding;

    public DateColumnWriter(TypeDescription type,  PixelsWriterOption writerOption)
    {
        super(type, writerOption);
        runlengthEncoding = encodingLevel.ge(EncodingLevel.EL2);
        if (runlengthEncoding)
        {
            // Issue #94: Date.getTime() can be negative if the date is before 1970-1-1.
            encoder = new RunLenIntEncoder(true, true);
        }
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
                if (nullsPadding)
                {
                    // padding 0 for nulls
                    curPixelVector[curPixelVectorIndex++] = 0;
                }
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
        if (runlengthEncoding)
        {
            for (int i = 0; i < curPixelVectorIndex; i++)
            {
                pixelStatRecorder.updateDate(curPixelVector[i]);
            }
            outputStream.write(encoder.encode(curPixelVector, 0, curPixelVectorIndex));
        }
        else
        {
            boolean littleEndian = byteOrder.equals(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < curPixelVectorIndex; i++)
            {
                if (littleEndian)
                {
                    encodingUtils.writeIntLE(outputStream, curPixelVector[i]);
                }
                else
                {
                    encodingUtils.writeIntBE(outputStream, curPixelVector[i]);
                }
                pixelStatRecorder.updateDate(curPixelVector[i]);
            }
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
