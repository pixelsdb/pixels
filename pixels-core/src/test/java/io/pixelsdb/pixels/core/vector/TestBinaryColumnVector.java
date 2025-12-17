/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.core.vector;

import org.junit.Assert;
import org.junit.Test;

public class TestBinaryColumnVector
{
    @Test
    public void testSerialize()
    {
        VectorizedRowBatch vectorizedRowBatch = new VectorizedRowBatch(1, 10240);
        BinaryColumnVector columnVector = new BinaryColumnVector(10240);
        vectorizedRowBatch.cols[0] = columnVector;
        int writeNum = 10000;
        for (int i = 0; i < writeNum; ++i)
        {
            columnVector.add("test" + i);
        }
        vectorizedRowBatch.size = writeNum;
        byte[] data = vectorizedRowBatch.serialize();
        int length = data.length;
        Assert.assertEquals(length < 4 * 1024 * 1024, true);
        VectorizedRowBatch newBatch = VectorizedRowBatch.deserialize(data);

        Assert.assertEquals(newBatch.size, writeNum);

        BinaryColumnVector col = (BinaryColumnVector) newBatch.cols[0];
        for (int i = 0; i < writeNum; ++i)
        {
            String decode = new String(col.vector[i], col.start[i], col.lens[i]);
            Assert.assertEquals(decode, "test" + i);
        }
    }
}
