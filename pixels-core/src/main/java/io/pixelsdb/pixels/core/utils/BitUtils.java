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

import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 * @author hank
 */
public class BitUtils
{
    private BitUtils()
    {
    }

    public static byte[] bitWiseCompact(boolean[] values, int length)
    {
        ByteArrayOutputStream bitWiseOutput = new ByteArrayOutputStream();
        // Issue #99: remove to improve performance.
        // int bitsToWrite = 1;
        int bitsLeft = 8;
        byte current = 0;

        for (int i = 0; i < length; i++)
        {
            byte v = values[i] ? (byte) 1 : (byte) 0;
            bitsLeft --; // -= bitsToWrite;
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
        // Issue #99: remove to improve performance.
        // int bitsToWrite = 1;
        int bitsLeft = 8;
        byte current = 0;

        for (int i = 0; i < length; i++)
        {
            byte v = values[i];
            bitsLeft --; // -= bitsToWrite;
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
     * @return de-compacted bits
     */
    public static void bitWiseDeCompact(byte[] bits, byte[] input)
    {
        /**
         * Issue #99:
         * Use as least as variables as possible to reduce stack footprint
         * and thus improve performance.
         */
         byte bitsLeft = 8;
         int index = 0;
        for (byte b : input)
        {
            while (bitsLeft > 0)
            {
                bitsLeft --;
                bits[index++] = (byte) (0x01 & (b >> bitsLeft));
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
     * @return de-compacted bits
     */
    public static void bitWiseDeCompact(byte[] bits, byte[] input, int offset, int length)
    {
        /**
         * Issue #99:
         * Use as least as variables as possible to reduce stack footprint
         * and thus improve performance.
         */
        byte bitsLeft = 8;
        int index = 0;
        for (int i = offset; i < offset + length; i++)
        {
            while (bitsLeft > 0)
            {
                bitsLeft --;
                bits[index++] = (byte)(0x01 & (input[i] >> bitsLeft));
            }
            bitsLeft = 8;
        }
    }

    /**
     * Bit de-compaction, this method does not modify the current position in input byte buffer.
     *
     * @param input input byte buffer, which can be direct.
     * @param offset starting offset of the input
     * @param length byte length of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompact(byte[] bits, ByteBuffer input, int offset, int length)
    {
        /**
         * Issue #99:
         * Use as least variables as possible to reduce stack footprint
         * and thus improve performance.
         */
        byte bitsLeft = 8, b;
        int index = 0;
        // loop condition fixed in Issue #99.
        for (int i = offset; i < offset + length; ++i)
        {
            b = input.get(i);
            while (bitsLeft > 0)
            {
                bitsLeft --;
                bits[index++] = (byte) (0x01 & (b >> bitsLeft));
            }
            bitsLeft = 8;
        }
    }

    /**
     * Bit de-compaction, this method does not modify the current position in input byte buffer.
     *
     * @param input input byte buffer, which can be direct.
     * @param offset starting offset of the input
     * @param length byte length of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompact(byte[] bits, ByteBuf input, int offset, int length)
    {
        /**
         * Issue #99:
         * Use as least as variables as possible to reduce stack footprint
         * and thus improve performance.
         */
        byte bitsLeft = 8, b;
        int index = 0;
        // loop condition fixed in Issue #99.
        for (int i = offset; i < offset + length; ++i)
        {
            b = input.getByte(i);
            while (bitsLeft > 0)
            {
                bitsLeft --;
                bits[index++] = (byte) (0x01 & (b >> bitsLeft));
            }
            bitsLeft = 8;
        }
    }
}
