/*
 * Copyright 2024 PixelsDB.
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
import io.pixelsdb.pixels.core.vector.VectorColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class VectorColumnReader extends ColumnReader
{

    private final int dimension;
    private ByteBuffer inputBuffer;
    private int inputIndex = 0;

    public VectorColumnReader(TypeDescription type)
    {
        super(type);
        this.dimension = type.getDimension();
    }

    /**
     * Read input buffer into a vector.
     *
     * @param input       input buffer
     * @param encoding    encoding type
     * @param offset      starting reading offset of values
     * @param size        number of values to read
     * @param pixelStride the stride (number of rows) in a pixel
     * @param vectorIndex the index from where we start reading values into the vector
     * @param vector      vector to read values into
     * @param chunkIndex  the metadata of the column chunk to read.
     */
    @Override
    public void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding, int offset, int size, int pixelStride,
                     int vectorIndex, ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex) throws IOException
    {
        VectorColumnVector vectorColumnVector = (VectorColumnVector) vector;
        ((VectorColumnVector) vector).vector = new double[vector.getLength()][((VectorColumnVector) vector).dimension];
        boolean nullsPadding = chunkIndex.hasNullsPadding() && chunkIndex.getNullsPadding();
        boolean littleEndian = chunkIndex.hasLittleEndian() && chunkIndex.getLittleEndian();
        if (offset == 0)
        // initialize
        {
            this.inputBuffer = input;
            //this.inputBuffer.order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
            inputIndex = inputBuffer.position();
            // isNull
            isNullOffset = inputIndex + chunkIndex.getIsNullOffset();
            isNullSkipBits = 0;
            // re-init
            hasNull = true;
            elementIndex = 0;
        }
        // elementIndex points at inputbuffer (?), and is about element instead of bytes (?)

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
                BitUtils.bitWiseDeCompact(vectorColumnVector.isNull, i, numToRead,
                        inputBuffer, isNullOffset, isNullSkipBits, littleEndian);
                isNullOffset += bytesToDeCompact;
                isNullSkipBits = (numToRead + isNullSkipBits) % 8;
                vectorColumnVector.noNulls = false;
            } else
            {
                Arrays.fill(vectorColumnVector.isNull, i, i + numToRead, false);
            }
            // read content
            if (nullsPadding)
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && vectorColumnVector.isNull[j]))
                    {
                        for (int d = 0; d < dimension; d++)
                        {
                            vectorColumnVector.vector[j][d] = inputBuffer.getDouble(inputIndex);
                            inputIndex += Double.BYTES;
                        }
                    }
                }
            } else
            {
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (!(hasNull && vectorColumnVector.isNull[j]))
                    {
                        for (int d = 0; d < dimension; d++)
                        {
                            vectorColumnVector.vector[j][d] = inputBuffer.getDouble(inputIndex);
                            inputIndex += Double.BYTES;
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

    @Override
    public void close() throws IOException
    {
    }
}
