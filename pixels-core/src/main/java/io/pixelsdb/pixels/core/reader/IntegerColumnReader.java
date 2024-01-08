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
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * @author guodong, hank
 * @create 2017-12-06
 * @update 2023-08-21: support nulls padding
 */
public class IntegerColumnReader extends ColumnReader
{
    private RunLenIntDecoder decoder;
    private ByteBuffer inputBuffer;
    private InputStream inputStream;

    /**
     * True if the data type of the values is long (int64), otherwise the data type is int32.
     */
    private boolean isLong = false;

    IntegerColumnReader(TypeDescription type)
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
        if (this.decoder != null)
        {
            // inputStream is closed inside decoder.close();
            this.decoder.close();
            this.decoder = null;
        }
        this.inputBuffer = null;
    }

    /**
     * Read input buffer.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param offset   starting reading offset of values
     * @param size     number of values to read
     * @param pixelStride the stride (number of rows) in a pixels.
     * @param vectorIndex the index from where we start reading values into the vector
     * @param vector   vector to read values into
     * @param chunkIndex the metadata of the column chunk to read.
     */
    @Override
    public void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, final int vectorIndex,
                     ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex) throws IOException
    {
        LongColumnVector columnVector = (LongColumnVector) vector;
        boolean decoding = encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH);
        boolean nullsPadding = chunkIndex.hasNullsPadding() && chunkIndex.getNullsPadding();
        boolean littleEndian = chunkIndex.hasLittleEndian() && chunkIndex.getLittleEndian();
        // if read from start, init the stream and decoder
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
            // isNull
            isNullOffset = inputBuffer.position() + chunkIndex.getIsNullOffset();
            isNullSkipBits = 0;
            // re-init
            hasNull = true;
            elementIndex = 0;
            if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.NONE))
            {
                isLong = type.getCategory() == TypeDescription.Category.LONG;
            }
        }

        // read without copying the de-compacted content and isNull
        int numLeft = size, numToRead, bytesToDeCompact;
        for (int i = vectorIndex; numLeft > 0; )
        {
            if (elementIndex / pixelStride < (elementIndex + numLeft) / pixelStride)
            {
                // read to the end of the current pixel
                numToRead = pixelStride - elementIndex % pixelStride;
            } else
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
            } else
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
                        columnVector.vector[j] = decoder.next();
                    }
                }
            } else
            {
                if (isLong)
                {
                    if (nullsPadding)
                    {
                        for (int j = i; j < i + numToRead; ++j)
                        {
                            columnVector.vector[j] = inputBuffer.getLong();
                        }
                    } else
                    {
                        for (int j = i; j < i + numToRead; ++j)
                        {
                            if (!(hasNull && columnVector.isNull[j]))
                            {
                                columnVector.vector[j] = inputBuffer.getLong();
                            }
                        }
                    }
                } else
                {
                    if (nullsPadding)
                    {
                        for (int j = i; j < i + numToRead; ++j)
                        {
                            columnVector.vector[j] = inputBuffer.getInt();
                        }
                    } else
                    {
                        for (int j = i; j < i + numToRead; ++j)
                        {
                            if (!(hasNull && columnVector.isNull[j]))
                            {
                                columnVector.vector[j] = inputBuffer.getInt();
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
}
