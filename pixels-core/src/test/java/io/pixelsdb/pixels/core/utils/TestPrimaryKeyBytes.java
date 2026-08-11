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
package io.pixelsdb.pixels.core.utils;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestPrimaryKeyBytes
{
    @Test
    public void sqlAndVectorPathsProduceSameKey()
    {
        TypeDescription schema = TypeDescription.fromString("struct<a:bigint,b:varchar(16)>");
        List<TypeDescription> pkTypes = schema.getChildren();

        String[] sqlValues = new String[]{"42", "hello"};
        byte[] fromSql = PrimaryKeyBytes.fromSqlStrings(pkTypes, sqlValues);

        VectorizedRowBatch batch = schema.createRowBatch(4);
        LongColumnVector a = (LongColumnVector) batch.cols[0];
        BinaryColumnVector b = (BinaryColumnVector) batch.cols[1];
        a.add(42L);
        b.add("hello");
        batch.size = 1;

        byte[] fromVector = PrimaryKeyBytes.fromColumnVectors(
                pkTypes, batch.cols, new int[]{0, 1}, 0);

        assertArrayEquals(fromSql, fromVector);
        assertEquals(Long.BYTES + "hello".getBytes(StandardCharsets.UTF_8).length, fromSql.length);
        assertEquals(42L, ByteBuffer.wrap(fromSql, 0, Long.BYTES).getLong());
    }

    @Test
    public void rejectsNullParts()
    {
        try
        {
            PrimaryKeyBytes.concat(new byte[]{1}, null);
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }

        TypeDescription schema = TypeDescription.fromString("struct<a:varchar>");
        try
        {
            PrimaryKeyBytes.fromSqlStrings(schema.getChildren(), new String[]{null});
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
    }

    @Test
    public void singleColumnReturnsSameArray()
    {
        byte[] part = ByteBuffer.allocate(Integer.BYTES).putInt(7).array();
        assertArrayEquals(part, PrimaryKeyBytes.concat(part));
    }

    @Test
    public void compositeConcatOrder()
    {
        byte[] left = new byte[]{1, 2};
        byte[] right = new byte[]{3};
        assertArrayEquals(new byte[]{1, 2, 3}, PrimaryKeyBytes.concat(left, right));
    }
}
