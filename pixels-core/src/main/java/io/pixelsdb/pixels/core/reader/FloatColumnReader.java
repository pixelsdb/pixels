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
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author guodong
 */
public class FloatColumnReader
        extends ColumnReader
{
    // private final EncodingUtils encodingUtils;
    private ByteBuffer inputBuffer;
    private byte[] isNull = new byte[8];
    private int inputIndex = 0;
    private int isNullOffset = 0;
    private int isNullBitIndex = 0;

    FloatColumnReader(TypeDescription type)
    {
        super(type);
        // this.encodingUtils = new EncodingUtils();
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
     * @param vectorIndex the starting index in the vector
     * @param vector   vector to read into
     * @param chunkIndex the metadata of the column chunk to read.
     */
    @Override
    public void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, final int vectorIndex,
                     ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex)
    {
        DoubleColumnVector columnVector = (DoubleColumnVector) vector;
        if (offset == 0)
        {
            this.inputBuffer = input;
            // using little endian, for that float is encoded into int by little endian
            this.inputBuffer.order(ByteOrder.LITTLE_ENDIAN);
            inputIndex = 0;
            isNullOffset = (int) chunkIndex.getIsNullOffset();
            hasNull = true;
            elementIndex = 0;
            isNullBitIndex = 8;
        }
        for (int i = 0; i < size; i++)
        {
            if (elementIndex % pixelStride == 0)
            {
                int pixelId = elementIndex / pixelStride;
                hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
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
                // columnVector.vector[i + vectorIndex] = encodingUtils.readIntLE(this.input, inputIndex);
                columnVector.vector[i + vectorIndex] = this.inputBuffer.getInt(inputIndex);
                inputIndex += 4;
            }
            if (hasNull)
            {
                isNullBitIndex++;
            }
            elementIndex++;
        }
    }
}
