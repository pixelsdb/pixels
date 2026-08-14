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
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.TimeColumnVector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static io.pixelsdb.pixels.core.utils.DatetimeUtils.PICOSECONDS_PER_MILLISECOND;

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
     * @param vector   vector to read values into, it is a {@link LongColumnVector} of picoseconds of day
     *                 if the read option requires the time column to be read as a long vector
     * @param chunkIndex the metadata of the column chunk to read.
     * @throws IOException
     */
    @Override
    public void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, final int vectorIndex,
                     ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex) throws IOException
    {
        // longColumnVector is not null if the time values are to be stored as picoseconds of day.
        LongColumnVector longColumnVector = vector instanceof LongColumnVector ? (LongColumnVector) vector : null;
        TimeColumnVector timeColumnVector = longColumnVector == null ? (TimeColumnVector) vector : null;
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
        boolean endOfPixels;
        for (int i = vectorIndex; numLeft > 0;)
        {
            if (elementIndex / pixelStride < (elementIndex + numLeft) / pixelStride)
            {
                // read to the end of the current pixel
                numToRead = pixelStride - elementIndex % pixelStride;
                endOfPixels = true;
            }
            else
            {
                numToRead = numLeft;
                endOfPixels = false;
            }
            bytesToDeCompact = (numToRead + isNullSkipBits + (endOfPixels ? 7 : 0)) / 8;
            // read isNull
            int pixelId = elementIndex / pixelStride;
            hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
            if (hasNull)
            {
                BitUtils.bitWiseDeCompact(vector.isNull, i, numToRead,
                        inputBuffer, isNullOffset, isNullSkipBits, littleEndian);
                isNullOffset += bytesToDeCompact;
                isNullSkipBits = endOfPixels ? 0 : (numToRead + isNullSkipBits) % 8;
                vector.noNulls = false;
            }
            else
            {
                Arrays.fill(vector.isNull, i, i + numToRead, false);
            }
            // read content
            if (decoding)
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && vector.isNull[j]))
                    {
                        int millis = (int) decoder.next();
                        if (longColumnVector != null)
                        {
                            longColumnVector.vector[j] = millis * PICOSECONDS_PER_MILLISECOND;
                            longColumnVector.isNull[j] = false;
                            if (j >= longColumnVector.getWriteIndex())
                            {
                                longColumnVector.setWriteIndex(j + 1);
                            }
                        }
                        else
                        {
                            timeColumnVector.set(j, millis);
                        }
                    }
                }
            }
            else
            {
                if (nullsPadding)
                {
                    for (int j = i; j < i + numToRead; ++j)
                    {
                        // Issue #791: do not call the set() method, as it may clear the isNull flag of null values.
                        int millis = inputBuffer.getInt();
                        if (longColumnVector != null)
                        {
                            longColumnVector.vector[j] = millis * PICOSECONDS_PER_MILLISECOND;
                        }
                        else
                        {
                            timeColumnVector.times[j] = millis;
                        }
                    }
                }
                else
                {
                    for (int j = i; j < i + numToRead; ++j)
                    {
                        if (!(hasNull && vector.isNull[j]))
                        {
                            // If time column is not encoded, it is written as integers instead of longs.
                            int millis = inputBuffer.getInt();
                            if (longColumnVector != null)
                            {
                                longColumnVector.vector[j] = millis * PICOSECONDS_PER_MILLISECOND;
                                longColumnVector.isNull[j] = false;
                                if (j >= longColumnVector.getWriteIndex())
                                {
                                    longColumnVector.setWriteIndex(j + 1);
                                }
                            }
                            else
                            {
                                timeColumnVector.set(j, millis);
                            }
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
     * @param vector   vector to read values into, it is a {@link LongColumnVector} of picoseconds of day
     *                 if the read option requires the time column to be read as a long vector
     * @param chunkIndex the metadata of the column chunk to read.
     * @param selected whether the value is selected, use the vectorIndex as the 0 offset of the selected
     * @throws IOException
     */
    @Override
    public void readSelected(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                             int offset, int size, int pixelStride, final int vectorIndex,
                             ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex, Bitmap selected) throws IOException
    {
        // longColumnVector is not null if the time values are to be stored as picoseconds of day.
        LongColumnVector longColumnVector = vector instanceof LongColumnVector ? (LongColumnVector) vector : null;
        TimeColumnVector timeColumnVector = longColumnVector == null ? (TimeColumnVector) vector : null;
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
        boolean endOfPixels;
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
                endOfPixels = true;
            }
            else
            {
                numToRead = numLeft;
                endOfPixels = false;
            }
            bytesToDeCompact = (numToRead + isNullSkipBits + (endOfPixels ? 7 : 0)) / 8;

            // read isNull
            int pixelId = elementIndex / pixelStride;
            hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
            if (hasNull)
            {
                if (!decoding && nullsPadding)
                {
                    // read isNull directly into the vector of the column chunk
                    BitUtils.bitWiseDeCompact(vector.isNull, vectorWriteIndex, numToRead, inputBuffer,
                            isNullOffset, isNullSkipBits, littleEndian, selected, i - vectorIndex);
                }
                else
                {
                    // need to keep isNull for later use
                    BitUtils.bitWiseDeCompact(isNull, i - vectorIndex, numToRead, inputBuffer,
                            isNullOffset, isNullSkipBits, littleEndian);
                    // update vector.isNull
                    int k = vectorWriteIndex;
                    for (int j = i; j < i + numToRead; ++j)
                    {
                        if (selected.get(j - vectorIndex))
                        {
                            vector.isNull[k++] = isNull[j - vectorIndex];
                        }
                    }
                }
                isNullOffset += bytesToDeCompact;
                isNullSkipBits = endOfPixels ? 0 : (numToRead + isNullSkipBits) % 8;
                vector.noNulls = false;
            }
            else
            {
                if (decoding || !nullsPadding)
                {
                    Arrays.fill(isNull, i - vectorIndex, i - vectorIndex + numToRead, false);
                }
                // update vector.isNull later to avoid bitmap unnecessary traversal
            }

            // read content
            int originalVectorWriteIndex = vectorWriteIndex;
            if (decoding)
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && isNull[j - vectorIndex]))
                    {
                        int millis = (int) decoder.next();
                        if (selected.get(j - vectorIndex))
                        {
                            if (longColumnVector != null)
                            {
                                longColumnVector.vector[vectorWriteIndex] = millis * PICOSECONDS_PER_MILLISECOND;
                                longColumnVector.isNull[vectorWriteIndex] = false;
                                if (vectorWriteIndex >= longColumnVector.getWriteIndex())
                                {
                                    longColumnVector.setWriteIndex(vectorWriteIndex + 1);
                                }
                                vectorWriteIndex++;
                            }
                            else
                            {
                                timeColumnVector.set(vectorWriteIndex++, millis);
                            }
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
                        int millis = inputBuffer.getInt();
                        if (selected.get(j - vectorIndex))
                        {
                            // Issue #791: do not call the set() method, as it may clear the isNull flag of null values.
                            if (longColumnVector != null)
                            {
                                longColumnVector.vector[vectorWriteIndex++] = millis * PICOSECONDS_PER_MILLISECOND;
                            }
                            else
                            {
                                timeColumnVector.times[vectorWriteIndex++] = millis;
                            }
                        }
                    }
                }
                else
                {
                    for (int j = i; j < i + numToRead; ++j)
                    {
                        if (!(hasNull && isNull[j - vectorIndex]))
                        {
                            // If time column is not encoded, it is written as integers instead of longs.
                            int millis = inputBuffer.getInt();
                            if (selected.get(j - vectorIndex))
                            {
                                if (longColumnVector != null)
                                {
                                    longColumnVector.vector[vectorWriteIndex] = millis * PICOSECONDS_PER_MILLISECOND;
                                    longColumnVector.isNull[vectorWriteIndex] = false;
                                    if (vectorWriteIndex >= longColumnVector.getWriteIndex())
                                    {
                                        longColumnVector.setWriteIndex(vectorWriteIndex + 1);
                                    }
                                    vectorWriteIndex++;
                                }
                                else
                                {
                                    timeColumnVector.set(vectorWriteIndex++, millis);
                                }
                            }
                        }
                        else if (selected.get(j - vectorIndex))
                        {
                            vectorWriteIndex++;
                        }
                    }
                }
            }

            // update vector.isNull if has no nulls
            if (!hasNull)
            {
                Arrays.fill(vector.isNull, originalVectorWriteIndex, vectorWriteIndex, false);
            }

            // update variables
            numLeft -= numToRead;
            elementIndex += numToRead;
            i += numToRead;
        }
    }
}
