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
import java.nio.ByteOrder;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The bit compaction and de-compaction utils.
 *
 * @author guodong
 * @author hank
 * @update 2023-08-26 Beijing: support endianness
 */
public class BitUtils
{
    private BitUtils() { }

    /**
     * Compact the values into bytes in the byte order. For big endian, the elements with low indexes in values
     * are compacted into the high bits in each byte, and vice versa.
     * @param values the boolean values to compact into bytes
     * @param length the number of elements in values to compact
     * @param byteOrder the byte order of the compact result
     * @return the compact result
     */
    public static byte[] bitWiseCompact(boolean[] values, int length, ByteOrder byteOrder)
    {
        if (byteOrder.equals(ByteOrder.LITTLE_ENDIAN))
        {
            return bitWiseCompactLE(values, length);
        }
        else
        {
            return bitWiseCompactBE(values, length);
        }
    }

    private static byte[] bitWiseCompactBE(boolean[] values, int length)
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

    private static byte[] bitWiseCompactLE(boolean[] values, int length)
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
     * Compact the values into bytes in the byte order. For big endian, the elements with low indexes in values
     * are compacted into the high bits in each byte, and vice versa.
     * @param values the boolean values to compact into bytes, each element should be 0 (false) or 1 (true)
     * @param length the number of elements in values to compact
     * @param byteOrder the byte order of the compact result
     * @return the compact result
     */
    public static byte[] bitWiseCompact(byte[] values, int length, ByteOrder byteOrder)
    {
        if (byteOrder.equals(ByteOrder.LITTLE_ENDIAN))
        {
            return bitWiseCompactLE(values, length);
        }
        else
        {
            return bitWiseCompactBE(values, length);
        }
    }

    private static byte[] bitWiseCompactBE(byte[] values, int length)
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

    private static byte[] bitWiseCompactLE(byte[] values, int length)
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
     * Bit de-compaction.
     *
     * @param bits de-compacted bits
     * @param input input byte array
     * @param littleEndian whether the byte order of input is little endian
     */
    public static void bitWiseDeCompact(byte[] bits, byte[] input, boolean littleEndian)
    {
        if (littleEndian)
        {
            bitWiseDeCompactLE(bits, input);
        }
        else
        {
            bitWiseDeCompactBE(bits, input);
        }
    }


    private static void bitWiseDeCompactBE(byte[] bits, byte[] input)
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

    private static void bitWiseDeCompactLE(byte[] bits, byte[] input)
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
     * Bit de-compaction.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param input  input byte array
     * @param offset starting offset of the input
     * @param length byte length of the input
     * @param littleEndian whether the byte order of input is little endian
     */
    public static void bitWiseDeCompact(byte[] bits, byte[] input, int offset, int length, boolean littleEndian)
    {
        checkArgument(offset >= 0 && length > 0, "invalid input offset or length");
        if (littleEndian)
        {
            bitWiseDeCompactLE(bits, input, offset, length);
        }
        else
        {
            bitWiseDeCompactBE(bits, input, offset, length);
        }
    }

    private static void bitWiseDeCompactBE(byte[] bits, byte[] input, int offset, int length)
    {
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

    private static void bitWiseDeCompactLE(byte[] bits, byte[] input, int offset, int length)
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
     * Bit de-compaction, this method does not modify the current position in input byte buffer.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @param length byte length of the input
     * @param littleEndian whether the byte order of input is little endian
     */
    public static void bitWiseDeCompact(byte[] bits, ByteBuffer input, int offset, int length, boolean littleEndian)
    {
        checkArgument(offset >= 0 && length > 0, "invalid input offset or length");
        if (littleEndian)
        {
            bitWiseDeCompactLE(bits, input, offset, length);
        }
        else
        {
            bitWiseDeCompactBE(bits, input, offset, length);
        }
    }

    private static void bitWiseDeCompactBE(byte[] bits, ByteBuffer input, int offset, int length)
    {
        byte bitsLeft = 8, b;
        int index = 0;
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

    private static void bitWiseDeCompactLE(byte[] bits, ByteBuffer input, int offset, int length)
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
     * Bit de-compaction, this method does not modify the current position in input byte buffer.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @param length byte length of the input
     * @param littleEndian whether the byte order of input is little endian
     */
    public static void bitWiseDeCompact(byte[] bits, ByteBuf input, int offset, int length, boolean littleEndian)
    {
        checkArgument(offset >= 0 && length > 0, "invalid input offset or length");
        if (littleEndian)
        {
            bitWiseDeCompactLE(bits, input, offset, length);
        }
        else
        {
            bitWiseDeCompactBE(bits, input, offset, length);
        }
    }

    private static void bitWiseDeCompactBE(byte[] bits, ByteBuf input, int offset, int length)
    {
        byte bitsLeft = 8, b;
        int index = 0;
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

    private static void bitWiseDeCompactLE(byte[] bits, ByteBuf input, int offset, int length)
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
     * Bit de-compaction, this method does not modify the current position in input byte buffer.
     * It always starts from the first bit of a byte in the input.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param bitsOffset the index in bits to start de-compact into
     * @param bitsLength the number of bits to de-compact
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @param skipBits the number of bits to skip in the first byte read from input, should be < 8
     * @param littleEndian whether the byte order of input is little endian
     */
    public static void bitWiseDeCompact(byte[] bits, int bitsOffset, int bitsLength,
                                        ByteBuffer input, int offset, int skipBits, boolean littleEndian)
    {
        checkArgument(bitsOffset >= 0 && bitsLength > 0, "invalid bitsOffset or bitsLength");
        checkArgument(offset >= 0, "invalid input offset");
        checkArgument(skipBits >=0 && skipBits < 8, "skipBits is out of the range [0, 8)");
        if (littleEndian)
        {
            bitWiseDeCompactLE(bits, bitsOffset, bitsLength, input, offset, skipBits);
        }
        else
        {
            bitWiseDeCompactBE(bits, bitsOffset, bitsLength, input, offset,skipBits);
        }
    }

    private static void bitWiseDeCompactBE(byte[] bits, int bitsOffset, int bitsLength,
                                           ByteBuffer input, int offset, int skipBits)
    {
        byte bitsLeft = 8, b;
        int bitsEnd = bitsOffset + bitsLength;
        b = input.get(offset++);
        for (int i = 7; i >= 0 && bitsOffset < bitsEnd; --i)
        {
            if (skipBits-- > 0)
            {
                continue;
            }
            bits[bitsOffset++] = (byte) (0x01 & (b >> i));
        }
        for (int i = offset; bitsOffset < bitsEnd; ++i)
        {
            b = input.get(i);
            while (bitsLeft > 0 && bitsOffset < bitsEnd)
            {
                bitsLeft --;
                bits[bitsOffset++] = (byte) (0x01 & (b >> bitsLeft));
            }
            bitsLeft = 8;
        }
    }

    private static void bitWiseDeCompactLE(byte[] bits, int bitsOffset, int bitsLength,
                                           ByteBuffer input, int offset, int skipBits)
    {
        byte currBit = 0, b;
        int bitsEnd = bitsOffset + bitsLength;
        b = input.get(offset++);
        for (int i = 0; i < 8 && bitsOffset < bitsEnd; ++i)
        {
            if (skipBits-- > 0)
            {
                continue;
            }
            bits[bitsOffset++] = (byte) (0x01 & (b >> i));
        }
        for (int i = offset; bitsOffset < bitsEnd; ++i)
        {
            b = input.get(i);
            while (currBit < 8 && bitsOffset < bitsEnd)
            {
                bits[bitsOffset++] = (byte) (0x01 & (b >> currBit));
                currBit ++;
            }
            currBit = 0;
        }
    }

    /**
     * Bit de-compaction, this method does not modify the current position in input byte buffer.
     * It always starts from the first bit of a byte in the input.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param bitsOffset the index in bits to start de-compact into
     * @param bitsLength the number of bits to de-compact
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @param skipBits the number of bits to skip in the first byte read from input, should be < 8
     * @param littleEndian whether the byte order of input is little endian
     */
    public static void bitWiseDeCompact(boolean[] bits, int bitsOffset, int bitsLength,
                                        ByteBuffer input, int offset, int skipBits, boolean littleEndian)
    {
        checkArgument(bitsOffset >= 0 && bitsLength > 0, "invalid bitsOffset or bitsLength");
        checkArgument(offset >= 0, "invalid input offset");
        checkArgument(skipBits >=0 && skipBits < 8, "skipBits is out of the range [0, 8)");
        if (littleEndian)
        {
            bitWiseDeCompactLE(bits, bitsOffset, bitsLength, input, offset, skipBits);
        }
        else
        {
            bitWiseDeCompactBE(bits, bitsOffset, bitsLength, input, offset, skipBits);
        }
    }

    private static void bitWiseDeCompactBE(boolean[] bits, int bitsOffset, int bitsLength,
                                           ByteBuffer input, int offset, int skipBits)
    {
        byte bitsLeft = 8, b;
        int bitsEnd = bitsOffset + bitsLength;
        b = input.get(offset++);
        for (int i = 7; i >= 0 && bitsOffset < bitsEnd; --i)
        {
            if (skipBits-- > 0)
            {
                continue;
            }
            bits[bitsOffset++] = (0x01 & (b >> i)) == 1;
        }
        for (int i = offset; bitsOffset < bitsEnd; ++i)
        {
            b = input.get(i);
            while (bitsLeft > 0 && bitsOffset < bitsEnd)
            {
                bitsLeft --;
                bits[bitsOffset++] = (0x01 & (b >> bitsLeft)) == 1;
            }
            bitsLeft = 8;
        }
    }

    private static void bitWiseDeCompactLE(boolean[] bits, int bitsOffset, int bitsLength,
                                           ByteBuffer input, int offset, int skipBits)
    {
        byte currBit = 0, b;
        int bitsEnd = bitsOffset + bitsLength;
        b = input.get(offset++);
        for (int i = 0; i < 8 && bitsOffset < bitsEnd; ++i)
        {
            if (skipBits-- > 0)
            {
                continue;
            }
            bits[bitsOffset++] = (0x01 & (b >> i)) == 1;
        }
        for (int i = offset; bitsOffset < bitsEnd; ++i)
        {
            b = input.get(i);
            while (currBit < 8 && bitsOffset < bitsEnd)
            {
                bits[bitsOffset++] = (0x01 & (b >> currBit)) == 1;
                currBit ++;
            }
            currBit = 0;
        }
    }

    /**
     * Bit de-compaction, this method does not modify the current position in input byte buffer.
     * It always starts from the first bit of a byte in the input.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param bitsOffset the index in bits to start de-compact into
     * @param bitsLength the number of bits to de-compact
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @param skipBits the number of bits to skip in the first byte read from input, should be < 8
     * @param littleEndian whether the byte order of input is little endian
     */
    public static void bitWiseDeCompact(boolean[] bits, int bitsOffset, int bitsLength,
                                        ByteBuf input, int offset, int skipBits, boolean littleEndian)
    {
        checkArgument(bitsOffset >= 0 && bitsLength > 0, "invalid bitsOffset or bitsLength");
        checkArgument(offset >= 0, "invalid input offset");
        checkArgument(skipBits >=0 && skipBits < 8, "skipBits is out of the range [0, 8)");
        if (littleEndian)
        {
            bitWiseDeCompactLE(bits, bitsOffset, bitsLength, input, offset, skipBits);
        }
        else
        {
            bitWiseDeCompactBE(bits, bitsOffset, bitsLength, input, offset, skipBits);
        }
    }

    private static void bitWiseDeCompactBE(boolean[] bits, int bitsOffset, int bitsLength,
                                           ByteBuf input, int offset, int skipBits)
    {
        byte bitsLeft = 8, b;
        int bitsEnd = bitsOffset + bitsLength;
        b = input.getByte(offset++);
        for (int i = 7; i >= 0 && bitsOffset < bitsEnd; --i)
        {
            if (skipBits-- > 0)
            {
                continue;
            }
            bits[bitsOffset++] = (0x01 & (b >> i)) == 1;
        }
        for (int i = offset; bitsOffset < bitsEnd; ++i)
        {
            b = input.getByte(i);
            while (bitsLeft > 0 && bitsOffset < bitsEnd)
            {
                bitsLeft --;
                bits[bitsOffset++] = (0x01 & (b >> bitsLeft)) == 1;
            }
            bitsLeft = 8;
        }
    }

    private static void bitWiseDeCompactLE(boolean[] bits, int bitsOffset, int bitsLength,
                                           ByteBuf input, int offset, int skipBits)
    {
        byte currBit = 0, b;
        int bitsEnd = bitsOffset + bitsLength;
        b = input.getByte(offset++);
        for (int i = 0; i < 8 && bitsOffset < bitsEnd; ++i)
        {
            if (skipBits-- > 0)
            {
                continue;
            }
            bits[bitsOffset++] = (0x01 & (b >> i)) == 1;
        }
        for (int i = offset; bitsOffset < bitsEnd; ++i)
        {
            b = input.getByte(i);
            while (currBit < 8 && bitsOffset < bitsEnd)
            {
                bits[bitsOffset++] = (0x01 & (b >> currBit)) == 1;
                currBit ++;
            }
            currBit = 0;
        }
    }

    /**
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param bitsOffset the index in bits to start de-compact into
     * @param bitsLength the number of bits to de-compact
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @param skipBits the number of bits to skip in the first byte read from input, should be < 8
     * @param littleEndian whether the byte order of input is little endian
     * @param bitmap determine whether to read the value of the input
     * @param bitmapOffset the offset in the bitmap to start reading
     */
    public static int bitWiseDeCompact(boolean[] bits, int bitsOffset, int bitsLength,
                                        ByteBuffer input, int offset, int skipBits, boolean littleEndian,
                                        Bitmap bitmap, int bitmapOffset)
    {
        checkArgument(bitsOffset >= 0 && bitsLength > 0, "invalid bitsOffset or bitsLength");
        checkArgument(offset >= 0, "invalid input offset");
        checkArgument(skipBits >=0 && skipBits < 8, "skipBits is out of the range [0, 8)");
        checkArgument(bitmapOffset >= 0, "invalid bitmap offset");
        if (littleEndian)
        {
            return bitWiseDeCompactLE(bits, bitsOffset, bitsLength, input, offset, skipBits, bitmap, bitmapOffset);
        }
        else
        {
            return bitWiseDeCompactBE(bits, bitsOffset, bitsLength, input, offset, skipBits, bitmap, bitmapOffset);
        }
    }

    private static int bitWiseDeCompactBE(boolean[] bits, int bitsOffset, int bitsLength,
                                           ByteBuffer input, int offset, int skipBits,
                                           Bitmap bitmap, int bitmapOffset)
    {
        byte bitsLeft = 8, b;
        int bitmapEnd = bitmapOffset + bitsLength, originBitsOffset = bitsOffset;
        b = input.get(offset++);
        for (int i = 7; i >= 0 && bitmapOffset < bitmapEnd; --i)
        {
            if (skipBits-- > 0)
            {
                continue;
            }
            if (bitmap.get(bitmapOffset++))
            {
                bits[bitsOffset++] = (0x01 & (b >> i)) == 1;
            }
        }
        for (int i = offset; bitmapOffset < bitmapEnd; ++i)
        {
            b = input.get(i);
            while (bitsLeft > 0 && bitmapOffset < bitmapEnd)
            {
                bitsLeft --;
                if (bitmap.get(bitmapOffset++))
                {
                    bits[bitsOffset++] = (0x01 & (b >> bitsLeft)) == 1;
                }
            }
            bitsLeft = 8;
        }
        return bitsOffset - originBitsOffset;
    }

    private static int bitWiseDeCompactLE(boolean[] bits, int bitsOffset, int bitsLength,
                                           ByteBuffer input, int offset, int skipBits,
                                           Bitmap bitmap, int bitmapOffset)
    {
        byte currBit = 0, b;
        int bitmapEnd = bitmapOffset + bitsLength, originBitsOffset = bitsOffset;
        b = input.get(offset++);
        for (int i = 0; i < 8 && bitmapOffset < bitmapEnd; ++i)
        {
            if (skipBits-- > 0)
            {
                continue;
            }
            if (bitmap.get(bitmapOffset++))
            {
                bits[bitsOffset++] = (0x01 & (b >> i)) == 1;
            }
        }
        for (int i = offset; bitmapOffset < bitmapEnd; ++i)
        {
            b = input.get(i);
            while (currBit < 8 && bitmapOffset < bitmapEnd)
            {
                if (bitmap.get(bitmapOffset++))
                {
                    bits[bitsOffset++] = (0x01 & (b >> currBit)) == 1;
                }
                currBit ++;
            }
            currBit = 0;
        }
        return bitsOffset - originBitsOffset;
    }

    /**
     * Bit de-compaction, this method does not modify the current position in input byte buffer.
     * It always starts from the first bit of a byte in the input.
     *
     * @param bits the de-compact (decode) result, each element is either 0 (false) or 1 (true)
     * @param bitsOffset the index in bits to start de-compact into
     * @param bitsLength the number of bits to de-compact
     * @param input input byte buffer, which can be direct
     * @param offset starting offset of the input
     * @param skipBits the number of bits to skip in the first byte read from input, should be < 8
     * @param littleEndian whether the byte order of input is little endian
     * @param bitmap determine whether to read the value of the input
     * @param bitmapOffset the offset in the bitmap to start reading
     */
    public static int bitWiseDeCompact(byte[] bits, int bitsOffset, int bitsLength,
                                        ByteBuffer input, int offset, int skipBits, boolean littleEndian,
                                        Bitmap bitmap, int bitmapOffset)
    {
        checkArgument(bitsOffset >= 0 && bitsLength > 0, "invalid bitsOffset or bitsLength");
        checkArgument(offset >= 0, "invalid input offset");
        checkArgument(skipBits >=0 && skipBits < 8, "skipBits is out of the range [0, 8)");
        checkArgument(bitmapOffset >= 0, "invalid bitmap offset");
        if (littleEndian)
        {
            return bitWiseDeCompactLE(bits, bitsOffset, bitsLength, input, offset, skipBits, bitmap, bitmapOffset);
        }
        else
        {
            return bitWiseDeCompactBE(bits, bitsOffset, bitsLength, input, offset,skipBits, bitmap, bitmapOffset);
        }
    }

    private static int bitWiseDeCompactBE(byte[] bits, int bitsOffset, int bitsLength,
                                           ByteBuffer input, int offset, int skipBits,
                                           Bitmap bitmap, int bitmapOffset)
    {
        byte bitsLeft = 8, b;
        int bitmapEnd = bitmapOffset + bitsLength, originBitsOffset = bitsOffset;
        b = input.get(offset++);
        for (int i = 7; i >= 0 && bitmapOffset < bitmapEnd; --i)
        {
            if (skipBits-- > 0)
            {
                continue;
            }
            if (bitmap.get(bitmapOffset++))
            {
                bits[bitsOffset++] = (byte) (0x01 & (b >> i));
            }
        }
        for (int i = offset; bitmapOffset < bitmapEnd; ++i)
        {
            b = input.get(i);
            while (bitsLeft > 0 && bitmapOffset < bitmapEnd)
            {
                bitsLeft --;
                if (bitmap.get(bitmapOffset++))
                {
                    bits[bitsOffset++] = (byte) (0x01 & (b >> bitsLeft));
                }
            }
            bitsLeft = 8;
        }
        return bitsOffset - originBitsOffset;
    }

    private static int bitWiseDeCompactLE(byte[] bits, int bitsOffset, int bitsLength,
                                           ByteBuffer input, int offset, int skipBits,
                                           Bitmap bitmap, int bitmapOffset)
    {
        byte currBit = 0, b;
        int bitmapEnd = bitmapOffset + bitsLength, originBitsOffset = bitsOffset;
        b = input.get(offset++);
        for (int i = 0; i < 8 && bitmapOffset < bitmapEnd; ++i)
        {
            if (skipBits-- > 0)
            {
                continue;
            }
            if (bitmap.get(bitmapOffset++))
            {
                bits[bitsOffset++] = (byte) (0x01 & (b >> i));
            }
        }
        for (int i = offset; bitmapOffset < bitmapEnd; ++i)
        {
            b = input.get(i);
            while (currBit < 8 && bitmapOffset < bitmapEnd)
            {
                if (bitmap.get(bitmapOffset++))
                {
                    bits[bitsOffset++] = (byte) (0x01 & (b >> currBit));
                }
                currBit ++;
            }
            currBit = 0;
        }
        return bitsOffset - originBitsOffset;
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
