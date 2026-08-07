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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.RunLenIntDecoder;
import io.pixelsdb.pixels.core.utils.BitUtils;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.utils.ByteBufferInputStream;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.ShortColumnVector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * This is the column reader for short (int16) columns.
 * 
 * @author gengdy
 * @create 2026-08-07
 */
public class ShortColumnReader extends ColumnReader
{
    private RunLenIntDecoder decoder;
    private ByteBuffer inputBuffer;
    private InputStream inputStream;

    ShortColumnReader(TypeDescription type)
    {
        super(type);
    }

    @Override
    public void close() throws IOException
    {
        if (this.decoder != null)
        {
            this.decoder.close();
            this.decoder = null;
        }
        this.inputBuffer = null;
    }

    @Override
    public void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, final int vectorIndex,
                     ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex) throws IOException
    {
        ShortColumnVector columnVector = (ShortColumnVector) vector;
        boolean decoding = encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH);
        boolean nullsPadding = chunkIndex.hasNullsPadding() && chunkIndex.getNullsPadding();
        boolean littleEndian = chunkIndex.hasLittleEndian() && chunkIndex.getLittleEndian();
        if (offset == 0)
        {
            if (inputStream != null)
            {
                inputStream.close();
            }
            this.inputBuffer = input;
            this.inputBuffer.order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
            inputStream = new ByteBufferInputStream(inputBuffer, inputBuffer.position(), inputBuffer.limit());
            decoder = new RunLenIntDecoder(inputStream, true);
            isNullOffset = inputBuffer.position() + chunkIndex.getIsNullOffset();
            isNullSkipBits = 0;
            hasNull = true;
            elementIndex = 0;
        }

        int numLeft = size, numToRead, bytesToDeCompact;
        boolean endOfPixel;
        for (int i = vectorIndex; numLeft > 0; )
        {
            if (elementIndex / pixelStride < (elementIndex + numLeft) / pixelStride)
            {
                numToRead = pixelStride - elementIndex % pixelStride;
                endOfPixel = true;
            }
            else
            {
                numToRead = numLeft;
                endOfPixel = false;
            }
            bytesToDeCompact = (numToRead + isNullSkipBits + (endOfPixel ? 7 : 0)) / 8;
            int pixelId = elementIndex / pixelStride;
            hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
            if (hasNull)
            {
                BitUtils.bitWiseDeCompact(columnVector.isNull, i, numToRead,
                        inputBuffer, isNullOffset, isNullSkipBits, littleEndian);
                isNullOffset += bytesToDeCompact;
                isNullSkipBits = endOfPixel ? 0 : (numToRead + isNullSkipBits) % 8;
                columnVector.noNulls = false;
            }
            else
            {
                Arrays.fill(columnVector.isNull, i, i + numToRead, false);
            }

            if (decoding)
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && columnVector.isNull[j]))
                    {
                        columnVector.vector[j] = (short) decoder.next();
                    }
                }
            }
            else if (nullsPadding)
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    columnVector.vector[j] = inputBuffer.getShort();
                }
            }
            else
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && columnVector.isNull[j]))
                    {
                        columnVector.vector[j] = inputBuffer.getShort();
                    }
                }
            }
            numLeft -= numToRead;
            elementIndex += numToRead;
            i += numToRead;
        }
    }

    @Override
    public void readSelected(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                             int offset, int size, int pixelStride, final int vectorIndex,
                             ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex,
                             Bitmap selected) throws IOException
    {
        ShortColumnVector columnVector = (ShortColumnVector) vector;
        boolean decoding = encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH);
        boolean nullsPadding = chunkIndex.hasNullsPadding() && chunkIndex.getNullsPadding();
        boolean littleEndian = chunkIndex.hasLittleEndian() && chunkIndex.getLittleEndian();
        if (offset == 0)
        {
            if (inputStream != null)
            {
                inputStream.close();
            }
            this.inputBuffer = input;
            this.inputBuffer.order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
            inputStream = new ByteBufferInputStream(inputBuffer, inputBuffer.position(), inputBuffer.limit());
            decoder = new RunLenIntDecoder(inputStream, true);
            isNullOffset = inputBuffer.position() + chunkIndex.getIsNullOffset();
            isNullSkipBits = 0;
            hasNull = true;
            elementIndex = 0;
        }

        int numLeft = size, numToRead, bytesToDeCompact, vectorWriteIndex = vectorIndex;
        boolean[] isNull = null;
        boolean endOfPixel;
        if (decoding || !nullsPadding)
        {
            isNull = new boolean[size];
        }
        for (int i = vectorIndex; numLeft > 0; )
        {
            if (elementIndex / pixelStride < (elementIndex + numLeft) / pixelStride)
            {
                numToRead = pixelStride - elementIndex % pixelStride;
                endOfPixel = true;
            }
            else
            {
                numToRead = numLeft;
                endOfPixel = false;
            }
            bytesToDeCompact = (numToRead + isNullSkipBits + (endOfPixel ? 7 : 0)) / 8;
            int pixelId = elementIndex / pixelStride;
            hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
            if (hasNull)
            {
                if (!decoding && nullsPadding)
                {
                    BitUtils.bitWiseDeCompact(columnVector.isNull, vectorWriteIndex, numToRead, inputBuffer,
                            isNullOffset, isNullSkipBits, littleEndian, selected, i - vectorIndex);
                }
                else
                {
                    BitUtils.bitWiseDeCompact(isNull, i - vectorIndex, numToRead, inputBuffer,
                            isNullOffset, isNullSkipBits, littleEndian);
                    int k = vectorWriteIndex;
                    for (int j = i; j < i + numToRead; ++j)
                    {
                        if (selected.get(j - vectorIndex))
                        {
                            columnVector.isNull[k++] = isNull[j - vectorIndex];
                        }
                    }
                }
                isNullOffset += bytesToDeCompact;
                isNullSkipBits = endOfPixel ? 0 : (numToRead + isNullSkipBits) % 8;
                columnVector.noNulls = false;
            }
            else
            {
                if (decoding || !nullsPadding)
                {
                    Arrays.fill(isNull, i - vectorIndex, i - vectorIndex + numToRead, false);
                }
            }

            int originalVectorWriteIndex = vectorWriteIndex;
            if (decoding)
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && isNull[j - vectorIndex]))
                    {
                        short value = (short) decoder.next();
                        if (selected.get(j - vectorIndex))
                        {
                            columnVector.vector[vectorWriteIndex++] = value;
                        }
                    }
                    else if (selected.get(j - vectorIndex))
                    {
                        vectorWriteIndex++;
                    }
                }
            }
            else if (nullsPadding)
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    short value = inputBuffer.getShort();
                    if (selected.get(j - vectorIndex))
                    {
                        columnVector.vector[vectorWriteIndex++] = value;
                    }
                }
            }
            else
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && isNull[j - vectorIndex]))
                    {
                        short value = inputBuffer.getShort();
                        if (selected.get(j - vectorIndex))
                        {
                            columnVector.vector[vectorWriteIndex++] = value;
                        }
                    }
                    else if (selected.get(j - vectorIndex))
                    {
                        vectorWriteIndex++;
                    }
                }
            }

            if (!hasNull)
            {
                Arrays.fill(columnVector.isNull, originalVectorWriteIndex, vectorWriteIndex, false);
            }
            numLeft -= numToRead;
            elementIndex += numToRead;
            i += numToRead;
        }
    }
}
