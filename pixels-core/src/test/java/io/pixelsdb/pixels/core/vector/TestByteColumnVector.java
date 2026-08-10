/*
 * Copyright 2026 PixelsDB.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestByteColumnVector
{
    @Test
    public void testAddStringUsesFullByteRange()
    {
        ByteColumnVector vector = new ByteColumnVector(256);
        for (int value = Byte.MIN_VALUE; value <= Byte.MAX_VALUE; ++value)
        {
            vector.add(Integer.toString(value));
        }

        assertEquals(256, vector.getWriteIndex());
        for (int index = 0; index < 256; ++index)
        {
            assertEquals((byte) (index + Byte.MIN_VALUE), vector.vector[index]);
            assertFalse(vector.isNull[index]);
        }
        vector.close();
    }

    @Test
    public void testAddCanonicalBytesAndNulls()
    {
        ByteColumnVector vector = new ByteColumnVector(4);
        vector.add(new byte[]{42});
        vector.add((byte[]) null);
        vector.add(new byte[0]);

        assertEquals(3, vector.getWriteIndex());
        assertEquals(42, vector.vector[0]);
        assertFalse(vector.isNull[0]);
        assertTrue(vector.isNull[1]);
        assertTrue(vector.isNull[2]);
        assertFalse(vector.noNulls);
        vector.close();
    }
}
