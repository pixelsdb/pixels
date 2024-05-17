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
package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.EncodingUtils;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

public class VectorColumnWriter extends BaseColumnWriter
{
    private final EncodingUtils encodingUtils;

    public VectorColumnWriter(TypeDescription type, PixelsWriterOption writerOption)
    {
        super(type, writerOption);
        encodingUtils = new EncodingUtils();
    }

    @Override
    public boolean decideNullsPadding(PixelsWriterOption writerOption)
    {
        return writerOption.isNullsPadding();
    }

    /**
     * Write a vector column vector to the output stream. The dimension of the vector should be enforced by the schema.
     *
     * @param vector vector
     * @param size   size of vector
     * @return
     * @throws IOException
     */
    @Override
    public int write(ColumnVector vector, int size) throws IOException
    {
        VectorColumnVector columnVector = (VectorColumnVector) vector;
        double[][] values = columnVector.vector;
        int curPartLength;
        int curPartOffset = 0;
        int nextPartLength = size;

        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
        {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartVec(columnVector, values, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurPartVec(columnVector, values, curPartLength, curPartOffset);

        return outputStream.size();
    }

    private void writeCurPartVec(VectorColumnVector columnVector, double[][] values,
                                 int curPartLength, int curPartOffset) throws IOException
    {
        for (int i = 0; i < curPartLength; i++)
        {
            curPixelEleIndex++;
            if (columnVector.isNull[i + curPartOffset])
            {
                hasNull = true;
                pixelStatRecorder.increment();
            } else
            {
                byte[] bytesOfOneVec = vecToBytes(values[curPartOffset + i], columnVector.dimension);
                outputStream.write(bytesOfOneVec);
                pixelStatRecorder.updateVector();
            }
        }
        System.arraycopy(columnVector.isNull, curPartOffset, isNull, curPixelIsNullIndex, curPartLength);
        curPixelIsNullIndex += curPartLength;
    }

    private byte[] vecToBytes(double[] vec, int dimension)
    {
        assert (vec.length == dimension);
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES * vec.length);

        for (double value : vec)
        {
            buffer.putDouble(value);
        }

        return buffer.array();
    }
}
