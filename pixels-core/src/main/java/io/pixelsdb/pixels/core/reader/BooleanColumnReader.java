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
import io.pixelsdb.pixels.core.utils.BitUtils;
import io.pixelsdb.pixels.core.vector.ByteColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;

/**
 * @author guodong, hank
 */
public class BooleanColumnReader extends ColumnReader
{
    private ByteBuffer inputBuffer;
    private byte[] bits;
    /**
     * The index of {@link #bits} if the column chunk is not nulls-padded,
     * or the index of {@link #inputBuffer} if the column chunk is nulls-padded.
     */
    private int bitsOrInputIndex = 0;
    /**
     * The number of bits to skip in the first byte (start from bitsOrInputIndex)
     * when decompacting values from nulls-padded column chunk.
     */
    private int inputSkipBits = 0;

    BooleanColumnReader(TypeDescription type)
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
        this.bits = null;
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
        ByteColumnVector columnVector = (ByteColumnVector) vector;
        int bytesToDeCompact;
        boolean nullsPadding = chunkIndex.hasNullsPadding() && chunkIndex.getNullsPadding();
        boolean littleEndian = chunkIndex.hasLittleEndian() && chunkIndex.getLittleEndian();
        if (offset == 0)
        {
            // read content
            this.inputBuffer = input;
            if (!nullsPadding)
            {
                // Issue #545: de-compact the byte before isNullOffset instead of the whole chunk
                bytesToDeCompact = chunkIndex.getIsNullOffset();
                bits = new byte[bytesToDeCompact * 8];
                BitUtils.bitWiseDeCompact(bits, input, input.position(), bytesToDeCompact, littleEndian);
            }
            else
            {
                // we will try to de-compact directly into the vector of the column chunk
                bits = null;
            }
            // read isNull
            isNullOffset = input.position() + chunkIndex.getIsNullOffset();
            isNullSkipBits = 0;
            // re-init
            bitsOrInputIndex = 0;
            inputSkipBits = 0;
            hasNull = true;
            elementIndex = 0;
        }

        // read without copying the de-compacted content and isNull
        int numLeft = size, numToRead;
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

            // read isNull
            int pixelId = elementIndex / pixelStride;
            hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
            if (hasNull)
            {
                bytesToDeCompact = (numToRead + isNullSkipBits) / 8;
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
            if (nullsPadding)
            {
                bytesToDeCompact = (numToRead + inputSkipBits) / 8;
                BitUtils.bitWiseDeCompact(columnVector.vector, i, numToRead,
                        inputBuffer, bitsOrInputIndex, inputSkipBits, littleEndian);
                bitsOrInputIndex += bytesToDeCompact;
                inputSkipBits = (numToRead + inputSkipBits) % 8;
            }
            else
            {
                // Issue #615: no need to align bitsOrInputIndex to 8.
                // bitsOrInputIndex = (bitsOrInputIndex + 7) / 8 * 8;
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && columnVector.isNull[j]))
                    {
                        columnVector.vector[j] = bits[bitsOrInputIndex++];
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
                             ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex, BitSet selected)
    {
        throw new UnsupportedOperationException("Not implemented yet.");
        ByteColumnVector columnVector = (ByteColumnVector) vector;
        int bytesToDeCompact;
        boolean nullsPadding = chunkIndex.hasNullsPadding() && chunkIndex.getNullsPadding();
        boolean littleEndian = chunkIndex.hasLittleEndian() && chunkIndex.getLittleEndian();
        if (offset == 0)
        {
            // read content
            this.inputBuffer = input;
            if (!nullsPadding)
            {
                // Issue #545: de-compact the byte before isNullOffset instead of the whole chunk
                bytesToDeCompact = chunkIndex.getIsNullOffset();
                bits = new byte[bytesToDeCompact * 8];
                BitUtils.bitWiseDeCompact(bits, input, input.position(), bytesToDeCompact, littleEndian);
            }
            else
            {
                // we will try to de-compact directly into the vector of the column chunk
                bits = null;
            }
            // read isNull
            isNullOffset = input.position() + chunkIndex.getIsNullOffset();
            isNullSkipBits = 0;
            // re-init
            bitsOrInputIndex = 0;
            inputSkipBits = 0;
            hasNull = true;
            elementIndex = 0;
        }

        // read without copying the de-compacted content and isNull
        int numLeft = size, numToRead, vectorWriteIndex = vectorIndex;
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

            // read isNull
            int pixelId = elementIndex / pixelStride;
            hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
            if (hasNull)
            {
                bytesToDeCompact = (numToRead + isNullSkipBits) / 8;
                BitUtils.bitWiseDeCompact(columnVector.isNull, vectorWriteIndex, numToRead, inputBuffer,
                        isNullOffset, isNullSkipBits, littleEndian, selected, i - vectorIndex);
                isNullOffset += bytesToDeCompact;
                isNullSkipBits = (numToRead + isNullSkipBits) % 8;
                columnVector.noNulls = false;
            }
            else
            {
                Arrays.fill(columnVector.isNull, i, i + numToRead, false);
            }
            // read content
            if (nullsPadding)
            {
                bytesToDeCompact = (numToRead + inputSkipBits) / 8;
                BitUtils.bitWiseDeCompact(columnVector.vector, i, numToRead,
                        inputBuffer, bitsOrInputIndex, inputSkipBits, littleEndian);
                bitsOrInputIndex += bytesToDeCompact;
                inputSkipBits = (numToRead + inputSkipBits) % 8;
            }
            else
            {
                // Issue #615: no need to align bitsOrInputIndex to 8.
                // bitsOrInputIndex = (bitsOrInputIndex + 7) / 8 * 8;
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && columnVector.isNull[j]))
                    {
                        columnVector.vector[j] = bits[bitsOrInputIndex++];
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
