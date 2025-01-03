/*
 * Copyright 2022 PixelsDB.
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
import io.pixelsdb.pixels.core.utils.BitUtils;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.LongDecimalColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * The column reader of long decimals with max precision and scale 38.
 *
 * @author hank
 * @create 2022-07-03
 * @update 2023-08-20 Zermatt: support nulls padding
 */
public class LongDecimalColumnReader extends ColumnReader
{
    // private final EncodingUtils encodingUtils;
    private ByteBuffer inputBuffer;
    private int inputIndex = 0;

    LongDecimalColumnReader(TypeDescription type)
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
                     ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex)
    {
        LongDecimalColumnVector columnVector = (LongDecimalColumnVector) vector;
        if (type.getPrecision() != columnVector.getPrecision() || type.getScale() != columnVector.getScale())
        {
            throw new IllegalArgumentException("reader of long_decimal(" + type.getPrecision() + "," + type.getScale() +
                    ") does not match the column vector of long_decimal(" + columnVector.getPrecision() + "," +
                    columnVector.getScale() + ")");
        }
        boolean nullsPadding = chunkIndex.hasNullsPadding() && chunkIndex.getNullsPadding();
        boolean littleEndian = chunkIndex.hasLittleEndian() && chunkIndex.getLittleEndian();
        if (offset == 0)
        {
            this.inputBuffer = input;
            this.inputBuffer.order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
            inputIndex = inputBuffer.position();
            // isNull
            isNullOffset = inputIndex + chunkIndex.getIsNullOffset();
            isNullSkipBits = 0;
            // re-init
            hasNull = true;
            elementIndex = 0;
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
            if (nullsPadding)
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    int index = j << 1;
                    columnVector.vector[index] = inputBuffer.getLong(inputIndex);
                    inputIndex += Long.BYTES;
                    columnVector.vector[index + 1] = inputBuffer.getLong(inputIndex);
                    inputIndex += Long.BYTES;
                }
            } else
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && columnVector.isNull[j]))
                    {
                        int index = j << 1;
                        columnVector.vector[index] = this.inputBuffer.getLong(inputIndex);
                        inputIndex += Long.BYTES;
                        columnVector.vector[index + 1] = this.inputBuffer.getLong(inputIndex);
                        inputIndex += Long.BYTES;
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
                             ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex, Bitmap selected)
    {
        LongDecimalColumnVector columnVector = (LongDecimalColumnVector) vector;
        if (type.getPrecision() != columnVector.getPrecision() || type.getScale() != columnVector.getScale())
        {
            throw new IllegalArgumentException("reader of long_decimal(" + type.getPrecision() + "," + type.getScale() +
                    ") does not match the column vector of long_decimal(" + columnVector.getPrecision() + "," +
                    columnVector.getScale() + ")");
        }
        boolean nullsPadding = chunkIndex.hasNullsPadding() && chunkIndex.getNullsPadding();
        boolean littleEndian = chunkIndex.hasLittleEndian() && chunkIndex.getLittleEndian();
        if (offset == 0)
        {
            this.inputBuffer = input;
            this.inputBuffer.order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
            inputIndex = inputBuffer.position();
            // isNull
            isNullOffset = inputIndex + chunkIndex.getIsNullOffset();
            isNullSkipBits = 0;
            // re-init
            hasNull = true;
            elementIndex = 0;
        }

        // read without copying the de-compacted content and isNull
        int numLeft = size, numToRead, bytesToDeCompact, vectorWriteIndex = vectorIndex;
        boolean[] isNull = null;
        if (!nullsPadding)
        {
            isNull = new boolean[size];
        }
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
                if (nullsPadding)
                {
                    // read to the end of the current pixel
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
                if (!nullsPadding)
                {
                    Arrays.fill(isNull, i - vectorIndex, i - vectorIndex + numToRead, false);
                }
                // update columnVector.isNull later to avoid bitmap unnecessary traversal
            }

            // read content
            int originalVectorWriteIndex = vectorWriteIndex;
            if (nullsPadding)
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (selected.get(j - vectorIndex))
                    {
                        int index = vectorWriteIndex << 1;
                        columnVector.vector[index] = inputBuffer.getLong(inputIndex);
                        inputIndex += Long.BYTES;
                        columnVector.vector[index + 1] = inputBuffer.getLong(inputIndex);
                        inputIndex += Long.BYTES;
                        vectorWriteIndex++;
                    }
                    else
                    {
                        inputIndex += 2 * Long.BYTES;
                    }
                }
            }
            else
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && isNull[j - vectorIndex]))
                    {
                        if (selected.get(j - vectorIndex))
                        {
                            int index = vectorWriteIndex << 1;
                            columnVector.vector[index] = this.inputBuffer.getLong(inputIndex);
                            inputIndex += Long.BYTES;
                            columnVector.vector[index + 1] = this.inputBuffer.getLong(inputIndex);
                            inputIndex += Long.BYTES;
                            vectorWriteIndex++;
                        }
                        else
                        {
                            inputIndex += 2 * Long.BYTES;
                        }
                    }
                    else if (selected.get(j - vectorIndex))
                    {
                        vectorWriteIndex++;
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
