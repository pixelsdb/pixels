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
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * pixels column writer for <code>Varchar</code>
 *
 * @author guodong
 * @author hank
 */
public class VarcharColumnWriter extends StringColumnWriter
{
    // Implemented in Issue #100.

    /**
     * Max length of varchar. It is recorded in the file footer's schema.
     */
    private final int maxLength;
    private int numTruncated;

    public VarcharColumnWriter(TypeDescription type, int pixelStride, EncodingLevel encodingLevel, ByteOrder byteOrder)
    {
        super(type, pixelStride, encodingLevel, byteOrder);
        this.maxLength = type.getMaxLength();
        this.numTruncated = 0;
    }

    @Override
    public int write(ColumnVector vector, int length) throws IOException
    {
        BinaryColumnVector columnVector = (BinaryColumnVector) vector;
        int[] vLens = columnVector.lens;

        for (int i = 0; i < vLens.length; ++i)
        {
            if (vLens[i] > maxLength)
            {
                vLens[i] = maxLength;
                this.numTruncated++;
            }
        }

        return super.write(vector, length);
    }

    @Override
    public void reset()
    {
        super.reset();
        this.numTruncated = 0;
    }

    /**
     * Get the number of truncated values.
     * TODO: report it as a warning in Pixels and other query engines.
     * @return
     */
    public int getNumTruncated ()
    {
        return this.numTruncated;
    }
}
