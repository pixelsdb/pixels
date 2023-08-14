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
package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.EncodingUtils;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DecimalColumnVector;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * The column writer of decimals.
 * <p><b>Note: it only supports short decimals with max precision and scale 18.</b></p>
 *
 * @author hank
 */
public class DecimalColumnWriter extends BaseColumnWriter
{
    private final EncodingUtils encodingUtils;

    public DecimalColumnWriter(TypeDescription type,  PixelsWriterOption writerOption)
    {
        super(type, writerOption);
        encodingUtils = new EncodingUtils();
    }

    @Override
    public int write(ColumnVector vector, int length) throws IOException
    {
        DecimalColumnVector columnVector = (DecimalColumnVector) vector;
        long[] values = columnVector.vector;
        boolean littleEndian = this.byteOrder.equals(ByteOrder.LITTLE_ENDIAN);
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
                if (littleEndian)
                {
                    encodingUtils.writeLongLE(outputStream, values[i]);
                }
                else
                {
                    encodingUtils.writeLongBE(outputStream, values[i]);
                }
                pixelStatRecorder.updateInteger(values[i], 1);
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
