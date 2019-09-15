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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.EncodingUtils;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public class DoubleColumnWriter extends BaseColumnWriter
{
    private final EncodingUtils encodingUtils;

    public DoubleColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
        encodingUtils = new EncodingUtils();
    }

    @Override
    public int write(ColumnVector vector, int length)
            throws IOException
    {
        DoubleColumnVector columnVector = (DoubleColumnVector) vector;
        long[] values = columnVector.vector;
        for (int i = 0; i < length; i++)
        {
            isNull[curPixelIsNullIndex++] = vector.isNull[i];
            curPixelEleIndex++;
            if (vector.isNull[i])
            {
                hasNull = true;
                pixelStatRecorder.increment();
            }
            else
            {
                encodingUtils.writeLongLE(outputStream, values[i]);
                pixelStatRecorder.updateDouble(Double.longBitsToDouble(values[i]));
            }
            // if current pixel size satisfies the pixel stride, end the current pixel and start a new one
            if (curPixelEleIndex >= pixelStride)
            {
                newPixel();
            }
        }
        return outputStream.size();
    }
}
