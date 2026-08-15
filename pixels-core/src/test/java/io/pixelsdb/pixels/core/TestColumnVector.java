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
package io.pixelsdb.pixels.core;

import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;
import io.pixelsdb.pixels.core.vector.IntColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.ShortColumnVector;
import io.pixelsdb.pixels.core.vector.TimestampColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * pixels
 *
 * @author guodong
 */
public class TestColumnVector
{
    @Test
    public void testCVSet()
    {
        LongColumnVector a = new LongColumnVector(100);
        for (int i = 0; i < 100; i++)
        {
            a.vector[i] = i;
            a.isNull[i] = false;
        }
        a.setWriteIndex(100);

        LongColumnVector b = new LongColumnVector(100);
        for (int i = 0; i < 100; i++)
        {
            b.addElement(i, a);
        }

        assertEquals(100, b.getWriteIndex());
        assertTrue(b.noNulls);
        for (int i = 0; i < b.getWriteIndex(); i++)
        {
            assertFalse(b.isNull[i]);
            assertEquals(i, b.vector[i]);
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < b.getWriteIndex(); i++)
        {
            b.stringifyValue(sb, i);
            if (i + 1 < b.getWriteIndex())
            {
                sb.append('\n');
            }
        }
        String[] lines = sb.toString().split("\n");
        assertEquals(100, lines.length);
        assertEquals("0", lines[0]);
        assertEquals("99", lines[99]);
    }

    @Test
    public void testDateTimeTypes()
    {
        Date date = Date.valueOf("1900-12-31");
        assertEquals(Date.valueOf("1900-12-31"), date);
        assertTrue(date.getTime() < 0);

        Time time = Time.valueOf("23:59:59");
        assertEquals(Time.valueOf("23:59:59").toString(), time.toString());

        Timestamp timestamp = Timestamp.valueOf("2018-05-07 20:39:20");
        assertEquals(0, timestamp.getNanos());
        assertEquals(Timestamp.valueOf("2018-05-07 20:39:20"), timestamp);

        date = new Date(System.currentTimeMillis());
        assertNotNull(date.toString());
        time = new Time(System.currentTimeMillis());
        assertNotNull(time.toString());
    }

    @Test
    public void testCVCopyFrom()
    {
        int testNum = 1000_000;
        String mockSchema = "struct<a:short,b:int,c:long,d:double,e:string,f:timestamp>";

        VectorizedRowBatch srcRowBatch = TypeDescription.fromString(mockSchema).createRowBatch(testNum);
        ShortColumnVector src0 = (ShortColumnVector) srcRowBatch.cols[0];
        IntColumnVector src1 = (IntColumnVector) srcRowBatch.cols[1];
        LongColumnVector src2 = (LongColumnVector) srcRowBatch.cols[2];
        DoubleColumnVector src3 = (DoubleColumnVector) srcRowBatch.cols[3];
        BinaryColumnVector src4 = (BinaryColumnVector) srcRowBatch.cols[4];
        TimestampColumnVector src5 = (TimestampColumnVector) srcRowBatch.cols[5];

        VectorizedRowBatch dstRowBatch = TypeDescription.fromString(mockSchema).createRowBatch(testNum);
        ShortColumnVector dst0 = (ShortColumnVector) dstRowBatch.cols[0];
        IntColumnVector dst1 = (IntColumnVector) dstRowBatch.cols[1];
        LongColumnVector dst2 = (LongColumnVector) dstRowBatch.cols[2];
        DoubleColumnVector dst3 = (DoubleColumnVector) dstRowBatch.cols[3];
        BinaryColumnVector dst4 = (BinaryColumnVector) dstRowBatch.cols[4];
        TimestampColumnVector dst5 = (TimestampColumnVector) dstRowBatch.cols[5];

        for (int i = 0; i < testNum; i++)
        {
            src0.vector[i] = (short) i;
            src1.vector[i] = i;
            src2.vector[i] = i;
            src3.vector[i] = i;
            src4.setVal(i, String.valueOf(i).getBytes());
            src5.set(i, Timestamp.valueOf("2018-05-07 20:39:20"));
        }

        dst0.duplicate(src0);
        dst1.duplicate(src1);
        dst2.duplicate(src2);
        dst3.duplicate(src3);
        dst4.duplicate(src4);
        dst5.duplicate(src5);

        for (int i = 0; i < testNum; i++)
        {
            assertEquals((short) i, dst0.vector[i]);
            assertEquals(i, dst1.vector[i]);
            assertEquals(i, dst2.vector[i]);
            assertEquals(i * 1.0d, dst3.vector[i], 0);
            assertEquals(String.valueOf(i), dst4.toString(i));
            assertEquals(Timestamp.valueOf("2018-05-07 20:39:20"), dst5.asScratchTimestamp(i));
        }
    }

    @Test
    public void testColumnDuplication()
    {
        String mockSchema = "struct<a:int,b:string,c:double,d:int,a:int,b:string,e:boolean>";
        VectorizedRowBatch rowBatch = TypeDescription.fromString(mockSchema).createRowBatch();

        assertFalse(rowBatch.cols[0].duplicated);
        assertEquals(-1, rowBatch.cols[0].originVecId);
        assertFalse(rowBatch.cols[1].duplicated);
        assertEquals(-1, rowBatch.cols[1].originVecId);
        assertFalse(rowBatch.cols[2].duplicated);
        assertEquals(-1, rowBatch.cols[2].originVecId);
        assertFalse(rowBatch.cols[3].duplicated);
        assertEquals(-1, rowBatch.cols[3].originVecId);
        assertTrue(rowBatch.cols[4].duplicated);
        assertEquals(0, rowBatch.cols[4].originVecId);
        assertTrue(rowBatch.cols[5].duplicated);
        assertEquals(1, rowBatch.cols[5].originVecId);
        assertFalse(rowBatch.cols[6].duplicated);
        assertEquals(-1, rowBatch.cols[6].originVecId);
    }

    @Test
    public void testBytesColumnVector()
    {
        // Keep capacity large enough to avoid BinaryColumnVector.ensureSize growth;
        // the lens-copy fix lives on feature/binaryColumn.
        int capacity = 10000;
        BinaryColumnVector cv = new BinaryColumnVector(capacity);
        cv.init();
        assertTrue(cv.getLength() >= capacity);

        for (int i = 0; i < capacity; i++)
        {
            cv.add("13333333333333333333333333333334");   //32 bytes
        }
        assertEquals(capacity, cv.getWriteIndex());
        assertTrue(cv.noNulls);
        assertEquals(32, cv.lens[0]);
        assertEquals(32, cv.lens[capacity - 1]);
        assertEquals("13333333333333333333333333333334", cv.toString(0));
        assertEquals("13333333333333333333333333333334", cv.toString(capacity - 1));

        cv.reset();
        assertEquals(0, cv.getWriteIndex());
        assertTrue(cv.noNulls);
    }
}
