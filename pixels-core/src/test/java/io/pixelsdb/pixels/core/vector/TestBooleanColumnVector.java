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
import static org.junit.Assert.fail;

public class TestBooleanColumnVector
{
    @Test
    public void testAddStringUsesBooleanLiterals()
    {
        BooleanColumnVector vector = new BooleanColumnVector(4);
        vector.add("TrUe");
        vector.add("0");
        vector.add(" false ");
        vector.add("1");

        assertEquals(4, vector.getWriteIndex());
        assertEquals(1, vector.vector[0]);
        assertEquals(0, vector.vector[1]);
        assertEquals(0, vector.vector[2]);
        assertEquals(1, vector.vector[3]);
        assertFalse(vector.isNull[0]);
        vector.close();
    }

    @Test
    public void testAddStringRejectsNonBoolean()
    {
        BooleanColumnVector vector = new BooleanColumnVector(1);
        try
        {
            vector.add("2");
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
        vector.close();
    }

    @Test
    public void testIsByteColumnVectorForTrinoCompatibility()
    {
        BooleanColumnVector vector = new BooleanColumnVector(1);
        assertTrue(vector instanceof ByteColumnVector);
        vector.add(true);
        assertEquals(1, vector.vector[0]);
        vector.close();
    }
}
