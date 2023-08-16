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
package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;

import java.io.IOException;

/**
 * pixels binary column writer.
 * each element consists of content length and content binary.
 * TODO: this class is not yet finished.
 *
 * @author guodong
 * @author hank
 */
public class BinaryColumnWriter extends BaseColumnWriter
{
    /**
     * Max length of binary. It is recorded in the file footer's schema.
     */
    private final int maxLength;
    private int numTruncated;

    public BinaryColumnWriter(TypeDescription type,  PixelsWriterOption writerOption)
    {
        super(type, writerOption);
        this.maxLength = type.getMaxLength();
        this.numTruncated = 0;
    }

    @Override
    public int write(ColumnVector vector, int size) throws IOException
    {
        BinaryColumnVector columnVector = (BinaryColumnVector) vector;
        byte[][] values = columnVector.vector;
        int curPartLength;
        int curPartOffset = 0;
        int nextPartLength = size;

        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
        {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartBinary(columnVector, values, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurPartBinary(columnVector, values, curPartLength, curPartOffset);

        return outputStream.size();
    }

    private void writeCurPartBinary(BinaryColumnVector columnVector, byte[][] values,
                                    int curPartLength, int curPartOffset) throws IOException
    {
        for (int i = 0; i < curPartLength; i++)
        {
            curPixelEleIndex++;
            if (columnVector.isNull[i + curPartOffset])
            {
                hasNull = true;
                pixelStatRecorder.increment();
            }
            else
            {
                byte[] bytes = values[curPartOffset + i];
                if (bytes.length <= maxLength)
                {
                    outputStream.write(bytes.length);
                    outputStream.write(bytes);
                }
                else
                {
                    outputStream.write(maxLength);
                    outputStream.write(bytes, 0, maxLength);
                    numTruncated++;
                }
                pixelStatRecorder.updateBinary(bytes, 0, bytes.length, 1);
            }
        }
        System.arraycopy(columnVector.isNull, curPartOffset, isNull, curPixelIsNullIndex, curPartLength);
        curPixelIsNullIndex += curPartLength;
    }

    @Override
    public boolean decideNullsPadding(PixelsWriterOption writerOption)
    {
        return writerOption.isNullsPadding();
    }
}
