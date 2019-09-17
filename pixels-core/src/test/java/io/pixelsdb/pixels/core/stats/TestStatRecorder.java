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
package io.pixelsdb.pixels.core.stats;

import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;

/**
 * pixels
 *
 * @author guodong
 */
public class TestStatRecorder
{
    @Test
    public void testBinaryStatsRecorder()
    {
        BinaryStatsRecorder recorder = new BinaryStatsRecorder();
        byte[] byte0 = "this".getBytes();
        byte[] byte1 = "that".getBytes();
        byte[] byte2 = "abcd".getBytes();
        byte[] byte3 = "okay".getBytes();
        recorder.updateBinary(byte0, 0, 4, 1);
        recorder.updateBinary(byte1, 0, 4, 2);
        recorder.updateBinary(byte2, 0, 4, 3);
        recorder.updateBinary(byte3, 0, 4, 4);
        assertEquals(10, recorder.getNumberOfValues());
        assertEquals(40, recorder.getSum());

        BinaryStatsRecorder mRecorder = new BinaryStatsRecorder();
        byte[] byte4 = "yeah".getBytes();
        mRecorder.updateBinary(byte4, 0, 4, 1);
        recorder.merge(mRecorder);
        assertEquals(11, recorder.getNumberOfValues());
        assertEquals(44, recorder.getSum());
    }

    @Test
    public void testBooleanStatsRecorder()
    {
        BooleanStatsRecorder recorder = new BooleanStatsRecorder();
        recorder.updateBoolean(true, 1);
        recorder.updateBoolean(false, 2);
        recorder.updateBoolean(false, 3);
        recorder.updateBoolean(true, 4);
        assertEquals(10, recorder.getNumberOfValues());
        assertEquals(5, recorder.getFalseCount());
        assertEquals(5, recorder.getTrueCount());

        BooleanStatsRecorder mRecorder = new BooleanStatsRecorder();
        mRecorder.updateBoolean(true, 1);
        recorder.merge(mRecorder);
        assertEquals(11, recorder.getNumberOfValues());
        assertEquals(5, recorder.getFalseCount());
        assertEquals(6, recorder.getTrueCount());
    }

    @Test
    public void testDoubleStatsRecorder()
    {
        DoubleStatsRecorder recorder = new DoubleStatsRecorder();
        recorder.updateDouble(0.1d);
        recorder.updateDouble(1.1d);
        recorder.updateDouble(0.5d);
        recorder.updateDouble(-0.4d);
        assertEquals(4, recorder.getNumberOfValues());
        assertEquals(-0.4d, recorder.getMinimum(), 0.0001d);
        assertEquals(1.1d, recorder.getMaximum(), 0.0001d);
        assertEquals(1.3d, recorder.getSum(), 0.0001d);

        DoubleStatsRecorder mRecorder = new DoubleStatsRecorder();
        mRecorder.updateDouble(9.9d);
        recorder.merge(mRecorder);
        assertEquals(5, recorder.getNumberOfValues());
        assertEquals(-0.4d, recorder.getMinimum(), 0.0001d);
        assertEquals(9.9d, recorder.getMaximum(), 0.0001d);
        assertEquals(11.2d, recorder.getSum(), 0.0001d);
    }

    @Test
    public void testIntegerStatsRecorder()
    {
        IntegerStatsRecorder recorder = new IntegerStatsRecorder();
        recorder.updateInteger(1, 1);
        recorder.updateInteger(10, 2);
        recorder.updateInteger(-20, 3);
        recorder.updateInteger(3500, 4);
        assertEquals(10, recorder.getNumberOfValues());
        assertEquals(-20, recorder.getMinimum().longValue());
        assertEquals(3500, recorder.getMaximum().longValue());
        assertEquals(13961, recorder.getSum());

        IntegerStatsRecorder mRecorder = new IntegerStatsRecorder();
        mRecorder.updateInteger(4000, 1);
        recorder.merge(mRecorder);
        assertEquals(11, recorder.getNumberOfValues());
        assertEquals(-20, recorder.getMinimum().longValue());
        assertEquals(4000, recorder.getMaximum().longValue());
        assertEquals(17961, recorder.getSum());
    }

    @Test
    public void testTimestampStatsRecorder()
    {
        TimestampStatsRecorder recorder = new TimestampStatsRecorder();
        recorder.updateTimestamp(Timestamp.valueOf("2016-09-09 12:23:00").getTime());
        recorder.updateTimestamp(Timestamp.valueOf("2017-09-01 10:12:04").getTime());
        recorder.updateTimestamp(Timestamp.valueOf("1992-04-04 19:24:44").getTime());
        recorder.updateTimestamp(Timestamp.valueOf("2018-04-09 14:25:49").getTime());
        assertEquals(4, recorder.getNumberOfValues());
        assertEquals(Timestamp.valueOf("1992-04-04 19:24:44").getTime(), recorder.getMinimum().longValue());
        assertEquals(Timestamp.valueOf("2018-04-09 14:25:49").getTime(), recorder.getMaximum().longValue());

        TimestampStatsRecorder mRecorder = new TimestampStatsRecorder();
        mRecorder.updateTimestamp(Timestamp.valueOf("1990-03-02 10:10:10"));
        recorder.merge(mRecorder);
        assertEquals(5, recorder.getNumberOfValues());
        assertEquals(Timestamp.valueOf("1990-03-02 10:10:10").getTime(), recorder.getMinimum().longValue());
        assertEquals(Timestamp.valueOf("2018-04-09 14:25:49").getTime(), recorder.getMaximum().longValue());
    }

    @Test
    public void testStringStatsRecorder()
    {
        StringStatsRecorder recorder = new StringStatsRecorder();
        recorder.updateString("this", 1);
        recorder.updateString("that", 2);
        recorder.updateString("abcd", 3);
        recorder.updateString("okay", 4);
        assertEquals("abcd", recorder.getMinimum());
        assertEquals("this", recorder.getMaximum());
        assertEquals(10, recorder.getNumberOfValues());
        assertEquals(40, recorder.getSum());

        StringStatsRecorder mRecorder = new StringStatsRecorder();
        mRecorder.updateString("yeah", 1);
        recorder.merge(mRecorder);
        assertEquals("abcd", recorder.getMinimum());
        assertEquals("yeah", recorder.getMaximum());
        assertEquals(11, recorder.getNumberOfValues());
        assertEquals(44, recorder.getSum());
    }

    @Test
    public void testStringStatsRecorderWithNull()
    {
        StringStatsRecorder recorder = new StringStatsRecorder();
//        recorder.updateString("", 1);
        recorder.updateString("this", 1);
        System.out.println("Min: " + recorder.getMaximum());
        System.out.println("Max: " + recorder.getMinimum());
        recorder.updateString(new byte[0], 0, 0, 1);
        System.out.println("Min: " + recorder.getMaximum());
        System.out.println("Max: " + recorder.getMinimum());
    }
}
