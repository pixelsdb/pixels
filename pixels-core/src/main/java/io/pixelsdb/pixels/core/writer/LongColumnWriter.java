/*
 * Copyright 2026 PixelsDB.
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
 * You should have received a copy of the Affero GNU General Public License
 * along with Pixels.  If not, see 
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.encoding.RunLenIntEncoder;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This is the column writer for long (int64) columns.
 * 
 * @author gengdy
 * @created 2026-08-07
 */
public class LongColumnWriter extends BaseColumnWriter
{
    private final long[] curPixelVector = new long[pixelStride];
    private final boolean runlengthEncoding;

    public LongColumnWriter(TypeDescription type, PixelsWriterOption writerOption)
    {
        super(type, writerOption);
        runlengthEncoding = encodingLevel.ge(EncodingLevel.EL2);
        if (runlengthEncoding)
        {
            encoder = new RunLenIntEncoder();
        }
    }

    @Override
    public int write(ColumnVector vector, int size) throws IOException
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        int curPartLength;
        int curPartOffset = 0;
        int nextPartLength = size;
        while (curPixelIsNullIndex + nextPartLength >= pixelStride)
        {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPart(columnVector, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }
        writeCurPart(columnVector, nextPartLength, curPartOffset);
        return outputStream.size();
    }

    private void writeCurPart(LongColumnVector vector, int length, int offset)
    {
        for (int i = 0; i < length; i++)
        {
            curPixelEleIndex++;
            if (vector.isNull[i + offset])
            {
                hasNull = true;
                pixelStatRecorder.increment();
                if (nullsPadding)
                {
                    curPixelVector[curPixelVectorIndex++] = 0L;
                }
            }
            else
            {
                curPixelVector[curPixelVectorIndex++] = vector.vector[i + offset];
            }
        }
        System.arraycopy(vector.isNull, offset, isNull, curPixelIsNullIndex, length);
        curPixelIsNullIndex += length;
    }

    @Override
    void newPixel() throws IOException
    {
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
            ByteBuffer buffer = ByteBuffer.allocate(curPixelVectorIndex * Long.BYTES).order(byteOrder);
            for (int i = 0; i < curPixelVectorIndex; i++)
            {
                buffer.putLong(curPixelVector[i]);
                pixelStatRecorder.updateInteger(curPixelVector[i], 1);
            }
            outputStream.write(buffer.array());
        }
        super.newPixel();
    }

    @Override
    public PixelsProto.ColumnEncoding.Builder getColumnChunkEncoding()
    {
        return PixelsProto.ColumnEncoding.newBuilder().setKind(runlengthEncoding ?
                PixelsProto.ColumnEncoding.Kind.RUNLENGTH : PixelsProto.ColumnEncoding.Kind.NONE);
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
        return !writerOption.getEncodingLevel().ge(EncodingLevel.EL2) && writerOption.isNullsPadding();
    }
}
