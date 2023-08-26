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
 * The bit compaction and de-compaction utils.
 *
 * @author guodong
 * @author hank
 */
public class BitUtils
{
    private BitUtils()
    {
    }

    /**
     * Compact the values into bytes in big endian. The elements with low indexes in values
     * are compacted into the high bits in each byte.
     * @param values the boolean values to compact into bytes
     * @param length the number of elements in values to compact
     * @return the compact result
     */
    public static byte[] bitWiseCompactBE(boolean[] values, int length)
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

    /**
     * Compact the values into bytes in little endian. The elements with low indexes in values
     * are compacted into the low bits in each byte.
     * @param values the boolean values to compact into bytes
     * @param length the number of elements in values to compact
     * @return the compact result
     */
    public static byte[] bitWiseCompactLE(boolean[] values, int length)
    {
        ByteArrayOutputStream bitWiseOutput = new ByteArrayOutputStream();
        int currBit = 0;
        byte current = 0;

        for (int i = 0; i < length; i++)
        {
            byte v = values[i] ? (byte) 1 : (byte) 0;
            current |= v << currBit;
            currBit ++;
            if (currBit == 8)
            {
                bitWiseOutput.write(current);
                current = 0;
                currBit = 0;
            }
        }

        if (currBit != 0)
        {
            bitWiseOutput.write(current);
        }

        return bitWiseOutput.toByteArray();
    }

    /**
     * Compact the values into bytes in big endian. The elements with low indexes in values
     * are compacted into the high bits in each byte.
     * @param values the boolean values to compact into bytes, each element should be 0 (false) or 1 (true)
     * @param length the number of elements in values to compact
     * @return the compact result
     */
    public static byte[] bitWiseCompactBE(byte[] values, int length)
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
     * Compact the values into bytes in little endian. The elements with low indexes in values
     * are compacted into the low bits in each byte.
     * @param values the boolean values to compact into bytes, each element should be 0 (false) or 1 (true)
     * @param length the number of elements in values to compact
     * @return the compact result
     */
    public static byte[] bitWiseCompactLE(byte[] values, int length)
    {
        ByteArrayOutputStream bitWiseOutput = new ByteArrayOutputStream();
        int currBit = 0;
        byte current = 0;

        for (int i = 0; i < length; i++)
        {
            byte v = values[i];
            current |= v << currBit;
            currBit ++;
            if (currBit == 8)
            {
                bitWiseOutput.write(current);
                current = 0;
                currBit = 0;
            }
        }

        if (currBit != 0)
        {
            bitWiseOutput.write(current);
        }

        return bitWiseOutput.toByteArray();
    }

    /**
     * Bit de-compaction in big endian.
     *
     * @param input input byte array
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactBE(byte[] bits, byte[] input)
    {
        /*
         * Issue #99:
         * Use as fewer variables as possible to reduce stack footprint
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
     * Bit de-compaction in big endian.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param input  input byte array
     * @param offset starting offset of the input
     * @param length byte length of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactBE(byte[] bits, byte[] input, int offset, int length)
    {
        /*
         * Issue #99:
         * Use as fewer variables as possible to reduce stack footprint
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
     * Bit de-compaction in big endian, this method does not modify the current position in input byte buffer.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @param length byte length of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactBE(byte[] bits, ByteBuffer input, int offset, int length)
    {
        /*
         * Issue #99:
         * Use as fewer variables as possible to reduce stack footprint
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
     * Bit de-compaction in big endian, this method does not modify the current position in input byte buffer.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param input input byte buffer, which can be direct.
     * @param offset starting offset of the input
     * @param length byte length of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactBE(byte[] bits, ByteBuf input, int offset, int length)
    {
        /*
         * Issue #99:
         * Use as fewer variables as possible to reduce stack footprint
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

    /**
     * Bit de-compaction in big endian, this method does not modify the current position in input byte buffer.
     * It always starts from the first bit of a byte in the input.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param bitsOffset the index in bits to start de-compact into
     * @param bitsLength the number of bits to de-compact
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactBE(byte[] bits, int bitsOffset, int bitsLength, ByteBuffer input, int offset)
    {
        byte bitsLeft = 8, b;
        int bitsEnd = bitsOffset + bitsLength;
        for (int i = offset, bitsIndex = bitsOffset; bitsIndex < bitsEnd; ++i)
        {
            b = input.get(i);
            while (bitsLeft > 0 && bitsIndex < bitsEnd)
            {
                bitsLeft --;
                bits[bitsIndex++] = (byte) (0x01 & (b >> bitsLeft));
            }
            bitsLeft = 8;
        }
    }

    /**
     * Bit de-compaction in big endian, this method does not modify the current position in input byte buffer.
     * It always starts from the first bit of a byte in the input.
     *
     * @param bits the de-compact (decode) result, each element is true if the corresponding bit is 1
     * @param bitsOffset the index in bits to start de-compact into
     * @param bitsLength the number of bits to de-compact
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactBE(boolean[] bits, int bitsOffset, int bitsLength, ByteBuffer input, int offset)
    {
        byte bitsLeft = 8, b;
        int bitsEnd = bitsOffset + bitsLength;
        for (int i = offset, bitsIndex = bitsOffset; bitsIndex < bitsEnd; ++i)
        {
            b = input.get(i);
            while (bitsLeft > 0 && bitsIndex < bitsEnd)
            {
                bitsLeft --;
                bits[bitsIndex++] = (0x01 & (b >> bitsLeft)) == 1;
            }
            bitsLeft = 8;
        }
    }

    /**
     * Bit de-compaction in big endian, this method does not modify the current position in input byte buffer.
     * It always starts from the first bit of a byte in the input.
     *
     * @param bits the de-compact (decode) result, each element is true if the corresponding bit is 1
     * @param bitsOffset the index in bits to start de-compact into
     * @param bitsLength the number of bits to de-compact
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactBE(boolean[] bits, int bitsOffset, int bitsLength, ByteBuf input, int offset)
    {
        byte bitsLeft = 8, b;
        int bitsEnd = bitsOffset + bitsLength;
        for (int i = offset, bitsIndex = bitsOffset; bitsIndex < bitsEnd; ++i)
        {
            b = input.getByte(i);
            while (bitsLeft > 0 && bitsIndex < bitsEnd)
            {
                bitsLeft --;
                bits[bitsIndex++] = (0x01 & (b >> bitsLeft)) == 1;
            }
            bitsLeft = 8;
        }
    }

    /**
     * Bit de-compaction in little endian.
     *
     * @param input input byte array
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactLE(byte[] bits, byte[] input)
    {
        byte currBit = 0;
        int index = 0;
        for (byte b : input)
        {
            while (currBit < 8)
            {
                bits[index++] = (byte) (0x01 & (b >> currBit));
                currBit ++;
            }
            currBit = 0;
        }
    }

    /**
     * Bit de-compaction in little endian.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param input  input byte array
     * @param offset starting offset of the input
     * @param length byte length of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactLE(byte[] bits, byte[] input, int offset, int length)
    {
        byte currBit = 0;
        int index = 0;
        for (int i = offset; i < offset + length; i++)
        {
            while (currBit < 8)
            {
                bits[index++] = (byte)(0x01 & (input[i] >> currBit));
                currBit ++;
            }
            currBit = 0;
        }
    }

    /**
     * Bit de-compaction in little endian, this method does not modify the current position in input byte buffer.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @param length byte length of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactLE(byte[] bits, ByteBuffer input, int offset, int length)
    {
        byte currBit = 0, b;
        int index = 0;
        for (int i = offset; i < offset + length; ++i)
        {
            b = input.get(i);
            while (currBit < 8)
            {
                bits[index++] = (byte) (0x01 & (b >> currBit));
                currBit ++;
            }
            currBit = 0;
        }
    }

    /**
     * Bit de-compaction in little endian, this method does not modify the current position in input byte buffer.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param input input byte buffer, which can be direct.
     * @param offset starting offset of the input
     * @param length byte length of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactLE(byte[] bits, ByteBuf input, int offset, int length)
    {
        byte currBit = 0, b;
        int index = 0;
        for (int i = offset; i < offset + length; ++i)
        {
            b = input.getByte(i);
            while (currBit < 8)
            {
                bits[index++] = (byte) (0x01 & (b >> currBit));
                currBit ++;
            }
            currBit = 0;
        }
    }

    /**
     * Bit de-compaction in little endian, this method does not modify the current position in input byte buffer.
     * It always starts from the first bit of a byte in the input.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param bitsOffset the index in bits to start de-compact into
     * @param bitsLength the number of bits to de-compact
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactLE(byte[] bits, int bitsOffset, int bitsLength, ByteBuffer input, int offset)
    {
        byte currBit = 0, b;
        int bitsEnd = bitsOffset + bitsLength;
        for (int i = offset, bitsIndex = bitsOffset; bitsIndex < bitsEnd; ++i)
        {
            b = input.get(i);
            while (currBit < 8 && bitsIndex < bitsEnd)
            {
                bits[bitsIndex++] = (byte) (0x01 & (b >> currBit));
                currBit ++;
            }
            currBit = 0;
        }
    }

    /**
     * Bit de-compaction in little endian, this method does not modify the current position in input byte buffer.
     * It always starts from the first bit of a byte in the input.
     *
     * @param bits the de-compact (decode) result, each element is true if the corresponding bit is 1
     * @param bitsOffset the index in bits to start de-compact into
     * @param bitsLength the number of bits to de-compact
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactLE(boolean[] bits, int bitsOffset, int bitsLength, ByteBuffer input, int offset)
    {
        byte currBit = 0, b;
        int bitsEnd = bitsOffset + bitsLength;
        for (int i = offset, bitsIndex = bitsOffset; bitsIndex < bitsEnd; ++i)
        {
            b = input.get(i);
            while (currBit < 8 && bitsIndex < bitsEnd)
            {
                bits[bitsIndex++] = (0x01 & (b >> currBit)) == 1;
                currBit ++;
            }
            currBit = 0;
        }
    }

    /**
     * Bit de-compaction in little endian, this method does not modify the current position in input byte buffer.
     * It always starts from the first bit of a byte in the input.
     *
     * @param bits the de-compact (decode) result, each element is true if the corresponding bit is 1
     * @param bitsOffset the index in bits to start de-compact into
     * @param bitsLength the number of bits to de-compact
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @return de-compacted bits
     */
    public static void bitWiseDeCompactLE(boolean[] bits, int bitsOffset, int bitsLength, ByteBuf input, int offset)
    {
        byte currBit = 0, b;
        int bitsEnd = bitsOffset + bitsLength;
        for (int i = offset, bitsIndex = bitsOffset; bitsIndex < bitsEnd; ++i)
        {
            b = input.getByte(i);
            while (currBit < 8 && bitsIndex < bitsEnd)
            {
                bits[bitsIndex++] = (0x01 & (b >> currBit)) == 1;
                currBit ++;
            }
            currBit = 0;
        }
    }

    /**
     * Copied from io.airlift.slice.
     * Turns a long representing a sequence of 8 bytes read in little-endian order
     * into a number that when compared produces the same effect as comparing the
     * original sequence of bytes lexicographically
     */
    public static long longBytesToLong(long bytes)
    {
        return Long.reverseBytes(bytes) ^ Long.MIN_VALUE;
    }
}
