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
package io.pixelsdb.pixels.core.utils;

import java.io.ByteArrayOutputStream;

/**
 * pixels
 *
 * @author guodong
 */
public class BitUtils
{
    private BitUtils()
    {
    }

    public static byte[] bitWiseCompact(boolean[] values, int length)
    {
        ByteArrayOutputStream bitWiseOutput = new ByteArrayOutputStream();
        int bitsToWrite = 1;
        int bitsLeft = 8;
        byte current = 0;

        for (int i = 0; i < length; i++)
        {
            byte v = values[i] ? (byte) 1 : (byte) 0;
            bitsLeft -= bitsToWrite;
            current |= v << bitsLeft;
            if (bitsLeft == 0)
            {
                bitWiseOutput.write(current);
                current = 0;
                bitsLeft = 8;
            }
        }

        if (bitsLeft != 8)
        {
            bitWiseOutput.write(current);
        }

        return bitWiseOutput.toByteArray();
    }

//    public static byte[] bitWiseCompact(long[] values, int length)
//    {
//        ByteArrayOutputStream bitWiseOutput = new ByteArrayOutputStream();
//        int bitsToWrite = 1;
//        int bitsLeft = 8;
//        byte current = 0;
//
//        for (int i = 0; i < length; i++)
//        {
//            long v = values[i];
//            bitsLeft -= bitsToWrite;
//            current |= v << bitsLeft;
//            if (bitsLeft == 0)
//            {
//                bitWiseOutput.write(current);
//                current = 0;
//                bitsLeft = 8;
//            }
//        }
//
//        if (bitsLeft != 8)
//        {
//            bitWiseOutput.write(current);
//        }
//
//        return bitWiseOutput.toByteArray();
//    }

    public static byte[] bitWiseCompact(byte[] values, int length)
    {
        ByteArrayOutputStream bitWiseOutput = new ByteArrayOutputStream();
        int bitsToWrite = 1;
        int bitsLeft = 8;
        byte current = 0;

        for (int i = 0; i < length; i++)
        {
            byte v = values[i];
            bitsLeft -= bitsToWrite;
            current |= v << bitsLeft;
            if (bitsLeft == 0)
            {
                bitWiseOutput.write(current);
                current = 0;
                bitsLeft = 8;
            }
        }

        if (bitsLeft != 8)
        {
            bitWiseOutput.write(current);
        }

        return bitWiseOutput.toByteArray();
    }

    /**
     * Bit de-compaction
     *
     * @param input input byte array
     * @return result bits
     */
    public static void bitWiseDeCompact(byte[] bits, byte[] input)
    {
        int bitsToRead = 1;
        int bitsLeft = 8;
        int current;
        byte mask = 0x01;

        int index = 0;
        for (byte b : input)
        {
            while (bitsLeft > 0)
            {
                bitsLeft -= bitsToRead;
                current = mask & (b >> bitsLeft);
                bits[index] = (byte) current;
                index++;
            }
            bitsLeft = 8;
        }
    }

    /**
     * Bit de-compaction
     *
     * @param input  input byte array
     * @param offset starting offset of the input
     * @param length byte length of the input
     * @return isNull
     */
    public static void bitWiseDeCompact(byte[] isNull, byte[] input, int offset, int length)
    {
        int bitsToRead = 1;
        int bitsLeft = 8;
        int current;
        byte mask = 0x01;

        int index = 0;
        for (int i = offset; i < offset + length; i++)
        {
            while (bitsLeft > 0)
            {
                bitsLeft -= bitsToRead;
                current = mask & (input[i] >> bitsLeft);
                isNull[index++] = (byte) current;
            }
            bitsLeft = 8;
        }
    }
}
