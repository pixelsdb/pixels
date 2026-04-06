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
package io.pixelsdb.pixels.core;

import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.DecimalColumnVector;
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;
import io.pixelsdb.pixels.core.vector.FloatColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link TypeDescription#convertColumnVectorToByte(
 * io.pixelsdb.pixels.core.vector.ColumnVector, int)}.
 * Migrated from TestStorageGarbageCollector where they were misplaced.
 */
public class TestTypeDescriptionConvert
{
    @Test
    public void testConvertColumnVectorToByte_int()
    {
        LongColumnVector col = new LongColumnVector(2);
        col.vector[0] = 42;
        col.vector[1] = -1;

        byte[] bytes0 = TypeDescription.createInt().convertColumnVectorToByte(col, 0);
        assertEquals(Integer.BYTES, bytes0.length);
        assertEquals(42, ByteBuffer.wrap(bytes0).getInt());

        byte[] bytes1 = TypeDescription.createInt().convertColumnVectorToByte(col, 1);
        assertEquals(-1, ByteBuffer.wrap(bytes1).getInt());
    }

    @Test
    public void testConvertColumnVectorToByte_long()
    {
        LongColumnVector col = new LongColumnVector(1);
        col.vector[0] = Long.MAX_VALUE;

        byte[] bytes = TypeDescription.createLong().convertColumnVectorToByte(col, 0);
        assertEquals(Long.BYTES, bytes.length);
        assertEquals(Long.MAX_VALUE, ByteBuffer.wrap(bytes).getLong());
    }

    @Test
    public void testConvertColumnVectorToByte_float()
    {
        FloatColumnVector col = new FloatColumnVector(1);
        col.vector[0] = Float.floatToIntBits(3.14f);

        byte[] bytes = TypeDescription.createFloat().convertColumnVectorToByte(col, 0);
        assertEquals(Integer.BYTES, bytes.length);
        assertEquals(Float.floatToIntBits(3.14f), ByteBuffer.wrap(bytes).getInt());
    }

    @Test
    public void testConvertColumnVectorToByte_double()
    {
        DoubleColumnVector col = new DoubleColumnVector(1);
        col.vector[0] = Double.doubleToLongBits(2.718);

        byte[] bytes = TypeDescription.createDouble().convertColumnVectorToByte(col, 0);
        assertEquals(Long.BYTES, bytes.length);
        assertEquals(Double.doubleToLongBits(2.718), ByteBuffer.wrap(bytes).getLong());
    }

    @Test
    public void testConvertColumnVectorToByte_string()
    {
        BinaryColumnVector col = new BinaryColumnVector(2);
        byte[] hello = "hello".getBytes();
        byte[] world = "world!".getBytes();
        col.setVal(0, hello);
        col.setVal(1, world);

        byte[] bytes0 = TypeDescription.createVarchar(255).convertColumnVectorToByte(col, 0);
        assertTrue("string bytes must match", Arrays.equals(hello, bytes0));

        byte[] bytes1 = TypeDescription.createString().convertColumnVectorToByte(col, 1);
        assertTrue("string bytes must match", Arrays.equals(world, bytes1));
    }

    @Test
    public void testConvertColumnVectorToByte_shortDecimal()
    {
        DecimalColumnVector col = new DecimalColumnVector(10, 2);
        col.vector[0] = 12345L;

        TypeDescription decType = TypeDescription.createDecimal(10, 2);
        byte[] bytes = decType.convertColumnVectorToByte(col, 0);
        assertEquals(Long.BYTES, bytes.length);
        assertEquals(12345L, ByteBuffer.wrap(bytes).getLong());
    }

    @Test
    public void testConvertColumnVectorToByte_boolean()
    {
        LongColumnVector col = new LongColumnVector(2);
        col.vector[0] = 1;
        col.vector[1] = 0;

        byte[] bytes0 = TypeDescription.createBoolean().convertColumnVectorToByte(col, 0);
        assertEquals(1, bytes0.length);
        assertEquals(1, bytes0[0]);

        byte[] bytes1 = TypeDescription.createBoolean().convertColumnVectorToByte(col, 1);
        assertEquals(1, bytes1.length);
        assertEquals(0, bytes1[0]);
    }
}
