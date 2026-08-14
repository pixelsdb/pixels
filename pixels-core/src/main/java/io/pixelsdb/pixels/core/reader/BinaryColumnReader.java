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
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Column reader for BINARY and VARBINARY.
 * The payloads of the non-null values are concatenated in the content field, and their start offsets
 * are stored in the starts field, which is located by the integer at the end of the column chunk.
 * Null values have no content bytes and no start offset.
 *
 * @author guodong, gengdy
 */
public class BinaryColumnReader extends ColumnReader
{
    private ByteBuffer inputBuffer;
    /**
     * The start offset of the column chunk in the input buffer.
     */
    private int contentStart;
    /**
     * The offset in the input buffer of the next start offset to read from the starts field.
     */
    private int startsReadIndex;
    /**
     * The start offset of the current value in the content field.
     */
    private int currentStart;
    /**
     * The start offset of the next value in the content field.
     */
    private int nextStart;

    BinaryColumnReader(TypeDescription type)
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
        this.contentStart = 0;
        this.startsReadIndex = 0;
        this.currentStart = 0;
        this.nextStart = 0;
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
        BinaryColumnVector columnVector = (BinaryColumnVector) vector;
        boolean littleEndian = chunkIndex.hasLittleEndian() && chunkIndex.getLittleEndian();
        // if read from start, init the input buffer and locate the starts field
        if (offset == 0)
        {
            ByteOrder byteOrder = littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
            int inputOffset = input.position();
            int chunkLength = input.remaining();
            if (input.hasArray())
            {
                inputBuffer = input.order(byteOrder);
            }
            else
            {
                byte[] chunkArray = new byte[chunkLength];
                input.duplicate().get(chunkArray);
                inputBuffer = ByteBuffer.wrap(chunkArray).order(byteOrder);
                inputOffset = 0;
            }
            contentStart = inputOffset;
            // the starts field offset is the last integer of the column chunk
            int startsFieldOffset = inputBuffer.getInt(contentStart + chunkLength - Integer.BYTES);
            startsReadIndex = contentStart + startsFieldOffset;
            currentStart = 0;
            // read out the first start offset, which is 0
            nextStart = inputBuffer.getInt(startsReadIndex);
            startsReadIndex += Integer.BYTES;
            isNullOffset = inputOffset + chunkIndex.getIsNullOffset();
            isNullSkipBits = 0;
            elementIndex = 0;
        }

        // read without copying the de-compacted content and isNull
        byte[] contentArray = inputBuffer.array();
        int contentArrayOffset = inputBuffer.arrayOffset() + contentStart;
        int numLeft = size, numToRead, bytesToDeCompact;
        boolean endOfPixel;
        for (int i = vectorIndex; numLeft > 0; )
        {
            if (elementIndex / pixelStride < (elementIndex + numLeft) / pixelStride)
            {
                // read to the end of the current pixel
                numToRead = pixelStride - elementIndex % pixelStride;
                endOfPixel = true;
            }
            else
            {
                numToRead = numLeft;
                endOfPixel = false;
            }
            bytesToDeCompact = (numToRead + isNullSkipBits + (endOfPixel ? 7 : 0)) / 8;
            // read isNull
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
            // read content
            for (int j = i; j < i + numToRead; ++j)
            {
                if (hasNull && columnVector.isNull[j])
                {
                    continue;
                }
                currentStart = nextStart;
                nextStart = inputBuffer.getInt(startsReadIndex);
                startsReadIndex += Integer.BYTES;
                // use setRef instead of setVal to reduce memory copy
                columnVector.setRef(j, contentArray, contentArrayOffset + currentStart, nextStart - currentStart);
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
        BinaryColumnVector columnVector = (BinaryColumnVector) vector;
        boolean littleEndian = chunkIndex.hasLittleEndian() && chunkIndex.getLittleEndian();
        // if read from start, init the input buffer and locate the starts field
        if (offset == 0)
        {
            ByteOrder byteOrder = littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
            int inputOffset = input.position();
            int chunkLength = input.remaining();
            if (input.hasArray())
            {
                inputBuffer = input.order(byteOrder);
            }
            else
            {
                byte[] chunkArray = new byte[chunkLength];
                input.duplicate().get(chunkArray);
                inputBuffer = ByteBuffer.wrap(chunkArray).order(byteOrder);
                inputOffset = 0;
            }
            contentStart = inputOffset;
            // the starts field offset is the last integer of the column chunk
            int startsFieldOffset = inputBuffer.getInt(contentStart + chunkLength - Integer.BYTES);
            startsReadIndex = contentStart + startsFieldOffset;
            currentStart = 0;
            // read out the first start offset, which is 0
            nextStart = inputBuffer.getInt(startsReadIndex);
            startsReadIndex += Integer.BYTES;
            isNullOffset = inputOffset + chunkIndex.getIsNullOffset();
            isNullSkipBits = 0;
            elementIndex = 0;
        }

        // read without copying the de-compacted content and isNull
        byte[] contentArray = inputBuffer.array();
        int contentArrayOffset = inputBuffer.arrayOffset() + contentStart;
        int numLeft = size, numToRead, bytesToDeCompact, vectorWriteIndex = vectorIndex;
        boolean[] isNull = new boolean[size];
        boolean endOfPixel;
        for (int i = vectorIndex; numLeft > 0; )
        {
            if (elementIndex / pixelStride < (elementIndex + numLeft) / pixelStride)
            {
                // read to the end of the current pixel
                numToRead = pixelStride - elementIndex % pixelStride;
                endOfPixel = true;
            }
            else
            {
                numToRead = numLeft;
                endOfPixel = false;
            }
            bytesToDeCompact = (numToRead + isNullSkipBits + (endOfPixel ? 7 : 0)) / 8;

            // read isNull
            int pixelId = elementIndex / pixelStride;
            hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
            if (hasNull)
            {
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
                isNullOffset += bytesToDeCompact;
                isNullSkipBits = endOfPixel ? 0 : (numToRead + isNullSkipBits) % 8;
                columnVector.noNulls = false;
            }
            else
            {
                Arrays.fill(isNull, i - vectorIndex, i - vectorIndex + numToRead, false);
                // update columnVector.isNull later to avoid bitmap unnecessary traversal
            }

            // read content
            int originalVectorWriteIndex = vectorWriteIndex;
            for (int j = i; j < i + numToRead; ++j)
            {
                if (hasNull && isNull[j - vectorIndex])
                {
                    if (selected.get(j - vectorIndex))
                    {
                        vectorWriteIndex++;
                    }
                    continue;
                }

                // always consume the start offset so that the content cursor stays aligned
                currentStart = nextStart;
                nextStart = inputBuffer.getInt(startsReadIndex);
                startsReadIndex += Integer.BYTES;
                if (selected.get(j - vectorIndex))
                {
                    // use setRef instead of setVal to reduce memory copy
                    columnVector.setRef(vectorWriteIndex++, contentArray,
                            contentArrayOffset + currentStart, nextStart - currentStart);
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
