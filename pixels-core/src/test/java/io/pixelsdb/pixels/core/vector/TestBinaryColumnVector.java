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

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Thin unit tests for {@link BinaryColumnVector} behavior (no encode/decode).
 */
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
        assertTrue(data.length < 4 * 1024 * 1024);
        VectorizedRowBatch newBatch = VectorizedRowBatch.deserialize(data);

        assertEquals(writeNum, newBatch.size);

        BinaryColumnVector col = (BinaryColumnVector) newBatch.cols[0];
        for (int i = 0; i < writeNum; ++i)
        {
            String decode = new String(col.vector[i], col.start[i], col.lens[i]);
            assertEquals("test" + i, decode);
        }
    }

    @Test
    public void testAddNullAndSetRef()
    {
        BinaryColumnVector vector = new BinaryColumnVector(4);
        byte[] backing = new byte[] {0x11, 1, 2, 3, 0x22};
        vector.setRef(0, backing, 1, 3);
        vector.addNull();
        vector.add(new byte[] {9});
        vector.addNull();

        assertFalse(vector.isNull[0]);
        assertArrayEquals(new byte[] {1, 2, 3},
                Arrays.copyOfRange(vector.vector[0], vector.start[0], vector.start[0] + vector.lens[0]));
        assertTrue(vector.isNull[1]);
        assertFalse(vector.isNull[2]);
        assertEquals(1, vector.lens[2]);
        assertTrue(vector.isNull[3]);
    }

    @Test
    public void testEnsureSizePreservesLens()
    {
        BinaryColumnVector vector = new BinaryColumnVector(2);
        byte[] first = new byte[] {7, 8, 9};
        byte[] second = new byte[] {(byte) 0xAA, 4, 5, (byte) 0xBB};
        vector.setRef(0, first, 0, 3);
        vector.setRef(1, second, 1, 2);

        vector.ensureSize(8, true);

        assertEquals(3, vector.lens[0]);
        assertEquals(2, vector.lens[1]);
        assertArrayEquals(new byte[] {7, 8, 9},
                Arrays.copyOfRange(vector.vector[0], vector.start[0], vector.start[0] + vector.lens[0]));
        assertArrayEquals(new byte[] {4, 5},
                Arrays.copyOfRange(vector.vector[1], vector.start[1], vector.start[1] + vector.lens[1]));
    }
}
