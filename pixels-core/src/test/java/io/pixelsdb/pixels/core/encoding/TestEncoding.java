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
package io.pixelsdb.pixels.core.encoding;

import io.pixelsdb.pixels.core.TestParams;
import io.pixelsdb.pixels.core.utils.BitUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * pixels
 *
 * @author guodong
 */
public class TestEncoding
{
    @Test
    public void runLengthTest()
    {
        Random random = new Random();
        long[] values = new long[TestParams.rowNum];
        values[0] = 0;
        values[1] = -1;
        values[2] = -2;
        for (int i = 3; i < TestParams.rowNum; i++)
        {
            values[i] = random.nextInt();
        }
        long[] decoderValues = new long[TestParams.rowNum];
        RunLenIntEncoder encoder = new RunLenIntEncoder(true, true);
        try
        {
            byte[] bytes = encoder.encode(values);
            IntDecoder decoder = new RunLenIntDecoder(new ByteArrayInputStream(bytes), true);
            int i = 0;
            while (decoder.hasNext())
            {
                decoderValues[i++] = decoder.next();
            }
            System.out.println(bytes.length);
            assertArrayEquals(values, decoderValues);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void runLengthByteTest() throws IOException
    {
        System.out.println("=== RunLenByte empty ===");
        RunLenByteEncoder emptyEncoder = new RunLenByteEncoder();
        byte[] emptyEncoded = emptyEncoder.encode(new byte[0]);
        emptyEncoder.close();
        System.out.println("encoded length: " + emptyEncoded.length);
        assertEquals(0, emptyEncoded.length);
        try (RunLenByteDecoder emptyDecoder = new RunLenByteDecoder(new ByteArrayInputStream(emptyEncoded)))
        {
            assertFalse(emptyDecoder.hasNext());
            System.out.println("decoder.hasNext(): false (ok)");
        }

        System.out.println("=== RunLenByte mixed round-trip ===");
        // mixed: literals, repeats across BYTE_RLE_MAX_REPEAT_SIZE, and boundary values
        byte[] values = new byte[200];
        Arrays.fill(values, 0, 5, (byte) 1);
        Arrays.fill(values, 5, 140, (byte) 7); // crosses 130-repeat boundary
        values[140] = (byte) -128;
        values[141] = 0;
        values[142] = 127;
        for (int i = 143; i < values.length; i++)
        {
            values[i] = (byte) i;
        }
        System.out.println("input rows: " + values.length
                + " (5x1, 135x7 across 130-repeat boundary, then -128/0/127 + literals)");
        System.out.println("input sample [0..9]: " + Arrays.toString(Arrays.copyOfRange(values, 0, 10)));
        System.out.println("input sample [138..149]: " + Arrays.toString(Arrays.copyOfRange(values, 138, 150)));

        RunLenByteEncoder encoder = new RunLenByteEncoder();
        byte[] encoded = encoder.encode(values);
        encoder.close();
        System.out.println("encoded bytes: " + encoded.length
                + " (compression ratio " + String.format("%.2f", (double) encoded.length / values.length) + ")");

        byte[] decoded = new byte[values.length];
        try (RunLenByteDecoder decoder = new RunLenByteDecoder(new ByteArrayInputStream(encoded)))
        {
            for (int i = 0; i < values.length; i++)
            {
                decoded[i] = decoder.next();
            }
            assertFalse(decoder.hasNext());
        }
        System.out.println("decoded sample [0..9]: " + Arrays.toString(Arrays.copyOfRange(decoded, 0, 10)));
        System.out.println("decoded sample [138..149]: " + Arrays.toString(Arrays.copyOfRange(decoded, 138, 150)));
        assertArrayEquals(values, decoded);
        System.out.println("round-trip OK");
    }

    @Test
    public void longTest()
    {
        long value = 0L;
        int high = (int) Math.pow(2, 30);
        long highL = (long) high;
        int low = 2;
        value |= (highL << 32);
        value |= low;
        System.out.println(value);

        long lowMask = 0x0000ffff;
        long highMask = 0xffff0000;
        int l = (int) (value & lowMask);
        long temp = (value & highMask);
        int h = (int) ((value & highMask) >>> 32);
        System.out.println("temp: " + temp + ", high: " + h + ", low: " + l);
    }

    @Test
    public void byteTest()
    {
        int[] bytes = new int[256];
        for (int i = 0; i < 256; i++)
        {
            int b = (Byte.MIN_VALUE + i) & 0xff;
            bytes[i] = b;
            System.out.println(b);
        }
        for (int i = 0; i < 256; i++)
        {
            byte b = (byte) (bytes[i]);
            System.out.println(b);
        }
    }

    @Test
    public void booleanBitWistCompactTest()
    {
        TestParams.rowNum = 300;
        boolean[] exp = new boolean[TestParams.rowNum];
        boolean[] cur = new boolean[TestParams.rowNum];
        for (int i = 0; i < TestParams.rowNum; i++)
        {
            cur[i] = i > 25;
            exp[i] = i > 25;
        }
        byte[] input = BitUtils.bitWiseCompact(cur, TestParams.rowNum, ByteOrder.BIG_ENDIAN);

        boolean[] res = new boolean[TestParams.rowNum];
        byte[] bytesRes = new byte[input.length * 8];
        BitUtils.bitWiseDeCompact(bytesRes, input, false);
        for (int i = 0; i < TestParams.rowNum; i++)
        {
            res[i] = bytesRes[i] == 1;
        }
        assertArrayEquals(exp, res);

        bytesRes = new byte[8];
        res = new boolean[8];
        BitUtils.bitWiseDeCompact(bytesRes, input, 3, 1, false);
        for (int i = 0; i < 8; i++)
        {
            res[i] = bytesRes[i] == 1;
        }
        exp = new boolean[]{false, false, true, true, true, true, true, true};
        assertArrayEquals(exp, res);
    }

    @Test
    public void byteBitWiseCompactTest()
    {
        TestParams.rowNum = 300;
        byte[] exp = new byte[TestParams.rowNum];
        byte[] cur = new byte[TestParams.rowNum];
        for (int i = 0; i < TestParams.rowNum; i++)
        {
            cur[i] = i > 25 ? (byte)1  : (byte) 0;
            exp[i] = i > 25 ? (byte)1  : (byte) 0;
        }

        byte[] compactedBytes = BitUtils.bitWiseCompact(cur, TestParams.rowNum, ByteOrder.BIG_ENDIAN);
        byte[] bytesRes = new byte[compactedBytes.length * 8];
        BitUtils.bitWiseDeCompact(bytesRes, compactedBytes, false);
        byte[] bytes = new byte[TestParams.rowNum];
        System.arraycopy(bytesRes, 0, bytes, 0, TestParams.rowNum);
        assertArrayEquals(exp, bytes);
    }
}
