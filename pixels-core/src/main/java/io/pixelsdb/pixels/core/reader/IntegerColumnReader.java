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
package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.RunLenIntDecoder;
import io.pixelsdb.pixels.core.utils.BitUtils;
import io.pixelsdb.pixels.core.utils.ByteBufferInputStream;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author guodong
 */
public class IntegerColumnReader
        extends ColumnReader
{
    private RunLenIntDecoder decoder;
    private ByteBuffer inputBuffer;
    private InputStream inputStream;
    private int isNullOffset = 0;
    private int isNullBitIndex = 0;
    private byte[] isNull = new byte[8];

    IntegerColumnReader(TypeDescription type)
    {
        super(type);
    }

    /**
     * Read input buffer.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param size     number of values to read
     * @param vector   vector to read into
     */
    @Override
    public void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, final int vectorIndex,
                     ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex)
            throws IOException
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        // if read from start, init the stream and decoder
        if (offset == 0)
        {
            if (inputStream != null)
            {
                inputStream.close();
            }
            this.inputBuffer = input;
            inputStream = new ByteBufferInputStream(inputBuffer, 0, inputBuffer.limit());
            decoder = new RunLenIntDecoder(inputStream, true);
            // isNull
            isNullOffset = (int) chunkIndex.getIsNullOffset();
            // re-init
            hasNull = true;
            elementIndex = 0;
            isNullBitIndex = 8;
        }
        // if run length encoded
        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH))
        {
            for (int i = 0; i < size; i++)
            {
                // if we're done with the current pixel, move to the next one
                if (elementIndex % pixelStride == 0)
                {
                    int pixelId = elementIndex / pixelStride;
                    hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                    if (hasNull && isNullBitIndex > 0)
                    {
                        BitUtils.bitWiseDeCompact(isNull, inputBuffer, isNullOffset++, 1);
                        isNullBitIndex = 0;
                    }
                }
                // if we're done with the current byte, move to the next one
                if (hasNull && isNullBitIndex >= 8)
                {
                    BitUtils.bitWiseDeCompact(isNull, inputBuffer, isNullOffset++, 1);
                    isNullBitIndex = 0;
                }
                // check if current offset is null
                if (hasNull && isNull[isNullBitIndex] == 1)
                {
                    columnVector.isNull[i + vectorIndex] = true;
                    columnVector.noNulls = false;
                }
                else
                {
                    columnVector.vector[i + vectorIndex] = decoder.next();
                }
                if (hasNull)
                {
                    isNullBitIndex++;
                }
                elementIndex++;
            }
        }
        // if not encoded
        else
        {
            byte firstByte = inputBuffer.get();
            boolean isLong = firstByte == (byte) 1;
            // if long
            if (isLong)
            {
                for (int i = 0; i < size; i++)
                {
                    if (elementIndex % pixelStride == 0)
                    {
                        int pixelId = elementIndex / pixelStride;
                        hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                        if (hasNull && isNullBitIndex > 0)
                        {
                            BitUtils.bitWiseDeCompact(isNull, inputBuffer, isNullOffset++, 1);
                            isNullBitIndex = 0;
                        }
                    }
                    if (hasNull && isNullBitIndex >= 8)
                    {
                        BitUtils.bitWiseDeCompact(isNull, inputBuffer, isNullOffset++, 1);
                        isNullBitIndex = 0;
                    }
                    if (hasNull && isNull[isNullBitIndex] == 1)
                    {
                        columnVector.isNull[i + vectorIndex] = true;
                        columnVector.noNulls = false;
                    }
                    else
                    {
                        columnVector.vector[i + vectorIndex] = inputBuffer.getLong();
                    }
                    if (hasNull)
                    {
                        isNullBitIndex++;
                    }
                    elementIndex++;
                }
            }
            // if int
            else
            {
                for (int i = 0; i < size; i++)
                {
                    if (elementIndex % pixelStride == 0)
                    {
                        int pixelId = elementIndex / pixelStride;
                        hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                        if (hasNull && isNullBitIndex > 0)
                        {
                            BitUtils.bitWiseDeCompact(isNull, inputBuffer, isNullOffset++, 1);
                            isNullBitIndex = 0;
                        }
                    }
                    if (hasNull && isNullBitIndex >= 8)
                    {
                        BitUtils.bitWiseDeCompact(isNull, inputBuffer, isNullOffset++, 1);
                        isNullBitIndex = 0;
                    }
                    if (hasNull && isNull[isNullBitIndex] == 1)
                    {
                        columnVector.isNull[i + vectorIndex] = true;
                        columnVector.noNulls = false;
                    }
                    else
                    {
                        columnVector.vector[i + vectorIndex] = inputBuffer.getInt();
                    }
                    if (hasNull)
                    {
                        isNullBitIndex++;
                    }
                    elementIndex++;
                }
            }
        }
    }
}
