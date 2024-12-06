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
package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.RunLenIntDecoder;
import io.pixelsdb.pixels.core.utils.BitUtils;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.utils.ByteBufferInputStream;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.TimeColumnVector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Pixels time column reader.
 * All time values are translated to the specified time zone after read from file.
 *
 * @author hank
 * @create 2021-04-28
 * @update 2023-08-20 Zermatt: support nulls padding
 */
public class TimeColumnReader extends ColumnReader
{
    private ByteBuffer inputBuffer = null;
    private InputStream inputStream = null;
    private RunLenIntDecoder decoder = null;

    TimeColumnReader(TypeDescription type)
    {
        super(type);
    }

    /**
     * Closes this column reader and releases any resources associated
     * with it. If the column reader is already closed then invoking this
     * method has no effect.
     * <p>
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException
    {
        if (inputStream != null)
        {
            inputStream.close();
        }
    }

    /**
     * Read values from input buffer.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param offset   starting reading offset of values
     * @param size     number of values to read
     * @param pixelStride the stride (number of rows) in a pixels.
     * @param vectorIndex the index from where we start reading values into the vector
     * @param vector   vector to read values into
     * @param chunkIndex the metadata of the column chunk to read.
     * @throws IOException
     */
    @Override
    public void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, final int vectorIndex,
                     ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex) throws IOException
    {
        TimeColumnVector columnVector = (TimeColumnVector) vector;
        boolean nullsPadding = chunkIndex.hasNullsPadding() && chunkIndex.getNullsPadding();
        boolean decoding = encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH);
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

        // read without copying the de-compacted content and isNull
        int numLeft = size, numToRead, bytesToDeCompact;
        for (int i = vectorIndex; numLeft > 0;)
        {
            if (elementIndex / pixelStride < (elementIndex + numLeft) / pixelStride)
            {
                // read to the end of the current pixel
                numToRead = pixelStride - elementIndex % pixelStride;
            }
            else
            {
                numToRead = numLeft;
            }
            bytesToDeCompact = (numToRead + isNullSkipBits) / 8;
            // read isNull
            int pixelId = elementIndex / pixelStride;
            hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
            if (hasNull)
            {
                BitUtils.bitWiseDeCompact(columnVector.isNull, i, numToRead,
                        inputBuffer, isNullOffset, isNullSkipBits, littleEndian);
                isNullOffset += bytesToDeCompact;
                isNullSkipBits = (numToRead + isNullSkipBits) % 8;
                columnVector.noNulls = false;
            }
            else
            {
                Arrays.fill(columnVector.isNull, i, i + numToRead, false);
            }
            // read content
            if (decoding)
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && columnVector.isNull[j]))
                    {
                        columnVector.set(j, (int) decoder.next());
                    }
                }
            }
            else
            {
                if (nullsPadding)
                {
                    for (int j = i; j < i + numToRead; ++j)
                    {
                        int millis = inputBuffer.getInt();
                        if (!(hasNull && columnVector.isNull[j]))
                        {
                            columnVector.set(j, millis);
                        }
                    }
                } else
                {
                    for (int j = i; j < i + numToRead; ++j)
                    {
                        if (!(hasNull && columnVector.isNull[j]))
                        {
                            // If time column is not encoded, it is written as integers instead of longs.
                            columnVector.set(j, inputBuffer.getInt());
                        }
                    }
                }
            }
            // update variables
            numLeft -= numToRead;
            elementIndex += numToRead;
            i += numToRead;
        }
    }

    /**
     * Read selected values from input buffer.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param offset   starting reading offset of values
     * @param size     number of values to read
     * @param pixelStride the stride (number of rows) in a pixels.
     * @param vectorIndex the index from where we start reading values into the vector
     * @param vector   vector to read values into
     * @param chunkIndex the metadata of the column chunk to read.
     * @param selected whether the value is selected, use the vectorIndex as the 0 offset of the selected
     * @throws IOException
     */
    @Override
    public void readSelected(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                             int offset, int size, int pixelStride, final int vectorIndex,
                             ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex, Bitmap selected) throws IOException
    {
        TimeColumnVector columnVector = (TimeColumnVector) vector;
        boolean nullsPadding = chunkIndex.hasNullsPadding() && chunkIndex.getNullsPadding();
        boolean decoding = encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH);
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

        // read without copying the de-compacted content and isNull
        int numLeft = size, numToRead, bytesToDeCompact, vectorWriteIndex = vectorIndex;
        boolean[] isNull = null;
        if (decoding || !nullsPadding)
        {
            isNull = new boolean[size];
        }
        for (int i = vectorIndex; numLeft > 0;)
        {
            if (elementIndex / pixelStride < (elementIndex + numLeft) / pixelStride)
            {
                // read to the end of the current pixel
                numToRead = pixelStride - elementIndex % pixelStride;
            }
            else
            {
                numToRead = numLeft;
            }
            bytesToDeCompact = (numToRead + isNullSkipBits) / 8;

            // read isNull
            int pixelId = elementIndex / pixelStride;
            hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
            if (hasNull)
            {
                if (!decoding && nullsPadding)
                {
                    // read isNull directly into the vector of the column chunk
                    BitUtils.bitWiseDeCompact(columnVector.isNull, vectorWriteIndex, numToRead, inputBuffer,
                            isNullOffset, isNullSkipBits, littleEndian, selected, i - vectorIndex);
                }
                else
                {
                    // need to keep isNull for later use
                    BitUtils.bitWiseDeCompact(isNull, i - vectorIndex, numToRead, inputBuffer,
                            isNullOffset, isNullSkipBits, littleEndian);
                    // update columnVector.isNull
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
                isNullSkipBits = (numToRead + isNullSkipBits) % 8;
                columnVector.noNulls = false;
            }
            else
            {
                if (decoding || !nullsPadding)
                {
                    Arrays.fill(isNull, i - vectorIndex, i - vectorIndex + numToRead, false);
                }
                // update columnVector.isNull later to avoid bitmap unnecessary traversal
            }

            // read content
            int originalVectorWriteIndex = vectorWriteIndex;
            if (decoding)
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && isNull[j - vectorIndex]))
                    {
                        int value = (int) decoder.next();
                        if (selected.get(j - vectorIndex))
                        {
                            columnVector.set(vectorWriteIndex++, value);
                        }
                    }
                    else if (selected.get(j - vectorIndex))
                    {
                        vectorWriteIndex++;
                    }
                }
            }
            else
            {
                if (nullsPadding)
                {
                    for (int j = i; j < i + numToRead; ++j)
                    {
                        int value = inputBuffer.getInt();
                        if (selected.get(j - vectorIndex))
                        {
                            columnVector.set(vectorWriteIndex++, value);
                        }
                    }
                }
                else
                {
                    for (int j = i; j < i + numToRead; ++j)
                    {
                        if (!(hasNull && isNull[j - vectorIndex]))
                        {
                            int value = inputBuffer.getInt();
                            if (selected.get(j - vectorIndex))
                            {
                                // If time column is not encoded, it is written as integers instead of longs.
                                columnVector.set(vectorWriteIndex++, value);
                            }
                        }
                        else if (selected.get(j - vectorIndex))
                        {
                            vectorWriteIndex++;
                        }
                    }
                }
            }

            // update columnVector.isNull if has no nulls
            if (!hasNull)
            {
                Arrays.fill(columnVector.isNull, originalVectorWriteIndex, vectorWriteIndex, false);
            }

            // update variables
            numLeft -= numToRead;
            elementIndex += numToRead;
            i += numToRead;
        }
    }
}
