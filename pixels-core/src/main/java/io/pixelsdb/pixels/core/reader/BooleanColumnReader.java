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

/**
 * @author guodong
 */
public class BooleanColumnReader extends ColumnReader
{
    private ByteBuffer inputBuffer;
    private byte[] bits;
    private byte[] isNull;
    /**
     * The index of {@link #bits} if the column chunk is not nulls-padded,
     * or the index of {@link #inputBuffer} if the column chunk is nulls-padded.
     */
    private int bitsOrInputIndex = 0;
    private int isNullOffset = 0;
    private int isNullBitIndex = 0;

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
        this.isNull = null;
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
        if (offset == 0)
        {
            // read content
            this.inputBuffer = input;
            if (!nullsPadding)
            {
                // Issue #545: de-compact the byte before isNullOffset instead of the whole chunk
                bytesToDeCompact = chunkIndex.getIsNullOffset();
                bits = new byte[bytesToDeCompact * 8];
                BitUtils.bitWiseDeCompact(bits, input, input.position(), bytesToDeCompact);
                isNull = new byte[8];
            }
            else
            {
                // we will try to de-compact directly into the vector of the column chunk
                bits = null;
                isNull = null;
            }
            // read isNull
            isNullOffset = input.position() + chunkIndex.getIsNullOffset();
            // re-init
            bitsOrInputIndex = 0;
            hasNull = true;
            elementIndex = 0;
            isNullBitIndex = 8;
        }
        if (nullsPadding)
        {
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
                bytesToDeCompact = (numToRead + 7) / 8;
                // read isNull
                int pixelId = elementIndex / pixelStride;
                hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                if (hasNull)
                {
                    BitUtils.bitWiseDeCompact(columnVector.isNull, i, numToRead, inputBuffer, isNullOffset);
                    isNullOffset += bytesToDeCompact;
                }
                else
                {
                    Arrays.fill(columnVector.isNull, i, i + numToRead, false);
                }
                // read content
                BitUtils.bitWiseDeCompact(columnVector.vector, i, numToRead, inputBuffer, bitsOrInputIndex);
                // update variables
                numLeft -= numToRead;
                elementIndex += numToRead;
                i += numToRead;
                bitsOrInputIndex += bytesToDeCompact;
            }
        }
        else
        {
            for (int i = 0; i < size; i++)
            {
                if (elementIndex % pixelStride == 0)
                {
                    int pixelId = elementIndex / pixelStride;
                    hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                    // skip the padding bits at the end of the previous pixel
                    bitsOrInputIndex = (bitsOrInputIndex + 7) / 8 * 8;
                    if (hasNull && isNullBitIndex > 0)
                    {
                        BitUtils.bitWiseDeCompact(this.isNull, inputBuffer, isNullOffset++, 1);
                        isNullBitIndex = 0;
                    }
                }
                if (hasNull && isNullBitIndex >= 8)
                {
                    BitUtils.bitWiseDeCompact(this.isNull, inputBuffer, isNullOffset++, 1);
                    isNullBitIndex = 0;
                }
                if (hasNull && isNull[isNullBitIndex] == 1)
                {
                    columnVector.isNull[i + vectorIndex] = true;
                    columnVector.noNulls = false;
                }
                else
                {
                    columnVector.vector[i + vectorIndex] = bits[bitsOrInputIndex++];
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
