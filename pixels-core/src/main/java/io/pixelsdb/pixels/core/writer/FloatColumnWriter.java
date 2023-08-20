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
import io.pixelsdb.pixels.core.utils.EncodingUtils;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.FloatColumnVector;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * The column writer of float.
 *
 * @author guodong, hank
 * @update 2023-08-16 Chamonix: support nulls padding
 * @update 2023-08-20 Palezieux: use FloatColumnVector instead of DoubleColumnVector
 */
public class FloatColumnWriter extends BaseColumnWriter
{
    private final EncodingUtils encodingUtils;

    public FloatColumnWriter(TypeDescription type,  PixelsWriterOption writerOption)
    {
        super(type, writerOption);
        encodingUtils = new EncodingUtils();
    }

    @Override
    public int write(ColumnVector vector, int length) throws IOException
    {
        FloatColumnVector columnVector = (FloatColumnVector) vector;
        int[] values = columnVector.vector;
        boolean littleEndian = this.byteOrder.equals(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < length; i++)
        {
            isNull[curPixelIsNullIndex++] = columnVector.isNull[i];
            curPixelEleIndex++;
            if (columnVector.isNull[i])
            {
                hasNull = true;
                pixelStatRecorder.increment();
                if (nullsPadding)
                {
                    // padding 0 for nulls
                    encodingUtils.writeIntLE(outputStream, 0);
                }
            }
            else
            {
                if (littleEndian)
                {
                    encodingUtils.writeIntLE(outputStream, values[i]);
                }
                else
                {
                    encodingUtils.writeIntBE(outputStream, values[i]);
                }
                pixelStatRecorder.updateFloat(Float.intBitsToFloat(values[i]));
            }
            // if current pixel size satisfies the pixel stride, end the current pixel and start a new one
            if (curPixelEleIndex >= pixelStride)
            {
                newPixel();
            }
        }
        return outputStream.size();
    }

    @Override
    public boolean decideNullsPadding(PixelsWriterOption writerOption)
    {
        return writerOption.isNullsPadding();
    }
}
