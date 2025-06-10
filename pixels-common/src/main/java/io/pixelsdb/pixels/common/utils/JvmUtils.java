/*
 * Copyright 2019-2022 PixelsDB.
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
package io.pixelsdb.pixels.common.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * The utilities related to JVM. Some of the fields and methods in this class are
 * from the source code of JDKs.
 *
 * @author hank
 * @create 2022-08-27
 */
public final class JvmUtils
{
    public static final Unsafe unsafe;
    public static final ByteOrder nativeOrder;
    public static int javaVersion = -1;
    public static final boolean nativeIsLittleEndian;

    private static final int[] supportedJavaVersions = {8, 11, 17, 21, 23};

    static
    {
        try
        {
            Field singleOneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
            singleOneInstanceField.setAccessible(true);
            unsafe = (Unsafe) singleOneInstanceField.get(null);
            nativeOrder = ByteOrder.nativeOrder();
            nativeIsLittleEndian = (nativeOrder == ByteOrder.LITTLE_ENDIAN);

            List<Integer> versionNumbers = new ArrayList<>();
            for (String v : System.getProperty("java.version").split("\\.|-"))
            {
                if (v.matches("\\d+"))
                {
                    versionNumbers.add(Integer.parseInt(v));
                }
            }
            if (versionNumbers.get(0) == 1)
            {
                if (versionNumbers.get(1) >= 8)
                {
                    javaVersion = versionNumbers.get(1);
                }
            } else if (versionNumbers.get(0) > 8)
            {
                javaVersion = versionNumbers.get(0);
            }
            if (javaVersion < 0)
            {
                throw new Exception(String.format("java version '%s' is not recognized", System.getProperty("java.version")));
            }
            boolean versionValid = false;
            for (int v : supportedJavaVersions)
            {
                if (v == javaVersion)
                {
                    versionValid = true;
                    break;
                }
            }
            if (!versionValid)
            {
                throw new Exception(String.format("java version '%s' is not supported", System.getProperty("java.version")));
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static final int LOG2_ARRAY_BOOLEAN_INDEX_SCALE = exactLog2(unsafe.ARRAY_BOOLEAN_INDEX_SCALE);
    public static final int LOG2_ARRAY_BYTE_INDEX_SCALE = exactLog2(unsafe.ARRAY_BYTE_INDEX_SCALE);
    public static final int LOG2_ARRAY_CHAR_INDEX_SCALE = exactLog2(unsafe.ARRAY_CHAR_INDEX_SCALE);
    public static final int LOG2_ARRAY_SHORT_INDEX_SCALE = exactLog2(unsafe.ARRAY_SHORT_INDEX_SCALE);
    public static final int LOG2_ARRAY_INT_INDEX_SCALE = exactLog2(unsafe.ARRAY_INT_INDEX_SCALE);
    public static final int LOG2_ARRAY_LONG_INDEX_SCALE = exactLog2(unsafe.ARRAY_LONG_INDEX_SCALE);
    public static final int LOG2_ARRAY_FLOAT_INDEX_SCALE = exactLog2(unsafe.ARRAY_FLOAT_INDEX_SCALE);
    public static final int LOG2_ARRAY_DOUBLE_INDEX_SCALE = exactLog2(unsafe.ARRAY_DOUBLE_INDEX_SCALE);
    public static final int LOG2_BYTE_BIT_SIZE = exactLog2(Byte.SIZE);

    private static int exactLog2(int scale)
    {
        if ((scale & (scale - 1)) != 0)
            throw new Error("data type scale not a power of two");
        return Integer.numberOfTrailingZeros(scale);
    }

    // Zero-extend an integer
    private static int toUnsignedInt(byte n)    { return n & 0xff; }
    private static int toUnsignedInt(short n)   { return n & 0xffff; }
    private static long toUnsignedLong(byte n)  { return n & 0xffl; }
    private static long toUnsignedLong(short n) { return n & 0xffffl; }
    private static long toUnsignedLong(int n)   { return n & 0xffffffffl; }

    // Maybe byte-reverse an integer
    private static char convEndian(boolean big, char n)   { return big != nativeIsLittleEndian ? n : Character.reverseBytes(n); }
    private static short convEndian(boolean big, short n) { return big != nativeIsLittleEndian ? n : Short.reverseBytes(n)    ; }
    private static int convEndian(boolean big, int n)     { return big != nativeIsLittleEndian ? n : Integer.reverseBytes(n)  ; }
    private static long convEndian(boolean big, long n)   { return big != nativeIsLittleEndian ? n : Long.reverseBytes(n)     ; }

    private static int pickPos(int top, int pos) { return nativeIsLittleEndian ? pos : top - pos; }

    /**
     * Fetches a value at some byte offset into a given Java object.
     * More specifically, fetches a value within the given object
     * <code>o</code> at the given offset, or (if <code>o</code> is
     * null) from the memory address whose numerical value is the
     * given offset.  <p>
     *
     * Unless <code>o</code> is null, the value accessed
     * must be entirely within the allocated object.  The endianness
     * of the value in memory is the endianness of the native platform.
     *
     * <p> The read will be atomic with respect to the largest power
     * of two that divides the GCD of the offset and the storage size.
     * For example, getLongUnaligned will make atomic reads of 2-, 4-,
     * or 8-byte storage units if the offset is zero mod 2, 4, or 8,
     * respectively.  There are no other guarantees of atomicity.
     * <p>
     * 8-byte atomicity is only guaranteed on platforms on which
     * support atomic accesses to longs.
     *
     * @param o Java heap object in which the value resides, if any, else
     *        null
     * @param offset The offset in bytes from the start of the object
     * @return the value fetched from the indicated object
     * @throws RuntimeException No defined exceptions are thrown, not even
     *         {@link NullPointerException}
     */
    public static long getLongUnaligned(Object o, long offset)
    {
        if ((offset & 7) == 0)
        {
            return unsafe.getLong(o, offset);
        }
        else if ((offset & 3) == 0)
        {
            return makeLong(unsafe.getInt(o, offset),
                    unsafe.getInt(o, offset + 4));
        }
        else if ((offset & 1) == 0)
        {
            return makeLong(unsafe.getShort(o, offset),
                    unsafe.getShort(o, offset + 2),
                    unsafe.getShort(o, offset + 4),
                    unsafe.getShort(o, offset + 6));
        }
        else
        {
            return makeLong(unsafe.getByte(o, offset),
                    unsafe.getByte(o, offset + 1),
                    unsafe.getByte(o, offset + 2),
                    unsafe.getByte(o, offset + 3),
                    unsafe.getByte(o, offset + 4),
                    unsafe.getByte(o, offset + 5),
                    unsafe.getByte(o, offset + 6),
                    unsafe.getByte(o, offset + 7));
        }
    }

    /**
     * As {@link #getLongUnaligned(Object, long)} but with an
     * additional argument which specifies the endianness of the value
     * as stored in memory.
     *
     * @param o Java heap object in which the variable resides
     * @param offset The offset in bytes from the start of the object
     * @param bigEndian The endianness of the value
     * @return the value fetched from the indicated object
     */
    public static long getLongUnaligned(Object o, long offset, boolean bigEndian)
    {
        return convEndian(bigEndian, getLongUnaligned(o, offset));
    }

    /** @see #getLongUnaligned(Object, long) */
    public static int getIntUnaligned(Object o, long offset)
    {
        if ((offset & 3) == 0)
        {
            return unsafe.getInt(o, offset);
        }
        else if ((offset & 1) == 0)
        {
            return makeInt(unsafe.getShort(o, offset),
                    unsafe.getShort(o, offset + 2));
        }
        else
        {
            return makeInt(unsafe.getByte(o, offset),
                    unsafe.getByte(o, offset + 1),
                    unsafe.getByte(o, offset + 2),
                    unsafe.getByte(o, offset + 3));
        }
    }
    /** @see #getLongUnaligned(Object, long, boolean) */
    public static int getIntUnaligned(Object o, long offset, boolean bigEndian)
    {
        return convEndian(bigEndian, getIntUnaligned(o, offset));
    }


    // These methods construct integers from bytes.  The byte ordering
    // is the native endianness of this platform.
    private static long makeLong(byte i0, byte i1, byte i2, byte i3, byte i4, byte i5, byte i6, byte i7)
    {
        return ((toUnsignedLong(i0) << pickPos(56, 0))
                | (toUnsignedLong(i1) << pickPos(56, 8))
                | (toUnsignedLong(i2) << pickPos(56, 16))
                | (toUnsignedLong(i3) << pickPos(56, 24))
                | (toUnsignedLong(i4) << pickPos(56, 32))
                | (toUnsignedLong(i5) << pickPos(56, 40))
                | (toUnsignedLong(i6) << pickPos(56, 48))
                | (toUnsignedLong(i7) << pickPos(56, 56)));
    }
    private static long makeLong(short i0, short i1, short i2, short i3)
    {
        return ((toUnsignedLong(i0) << pickPos(48, 0))
                | (toUnsignedLong(i1) << pickPos(48, 16))
                | (toUnsignedLong(i2) << pickPos(48, 32))
                | (toUnsignedLong(i3) << pickPos(48, 48)));
    }
    private static long makeLong(int i0, int i1)
    {
        return (toUnsignedLong(i0) << pickPos(32, 0))
                | (toUnsignedLong(i1) << pickPos(32, 32));
    }
    private static int makeInt(short i0, short i1)
    {
        return (toUnsignedInt(i0) << pickPos(16, 0))
                | (toUnsignedInt(i1) << pickPos(16, 16));
    }
    private static int makeInt(byte i0, byte i1, byte i2, byte i3)
    {
        return ((toUnsignedInt(i0) << pickPos(24, 0))
                | (toUnsignedInt(i1) << pickPos(24, 8))
                | (toUnsignedInt(i2) << pickPos(24, 16))
                | (toUnsignedInt(i3) << pickPos(24, 24)));
    }

    public static int mismatch(double[] a, double[] b, int length)
    {
        return mismatch(a, 0, b, 0, length);
    }

    public static int mismatch(double[] a, int aFromIndex,
                               double[] b, int bFromIndex, int length)
    {
        if (length == 0)
        {
            return -1;
        }
        int i = 0;
        if (Double.doubleToRawLongBits(a[aFromIndex]) == Double.doubleToRawLongBits(b[bFromIndex]))
        {
            int aOffset = unsafe.ARRAY_DOUBLE_BASE_OFFSET + (aFromIndex << LOG2_ARRAY_DOUBLE_INDEX_SCALE);
            int bOffset = unsafe.ARRAY_DOUBLE_BASE_OFFSET + (bFromIndex << LOG2_ARRAY_DOUBLE_INDEX_SCALE);
            i = vectorizedMismatch(a, aOffset, b, bOffset, length, LOG2_ARRAY_DOUBLE_INDEX_SCALE);
        }
        if (i >= 0)
        {
            // Check if mismatch is not associated with two NaN values
            if (!Double.isNaN(a[aFromIndex + i]) || !Double.isNaN(b[bFromIndex + i]))
            {
                return i;
            }

            // Mismatch on two different NaN values that are normalized to match
            // Fall back to slow mechanism
            // ISSUE: Consider looping over vectorizedMismatch adjusting ranges
            // However, requires that returned value be relative to input ranges
            i++;
            for (; i < length; i++)
            {
                if (Double.doubleToLongBits(a[aFromIndex + i]) != Double.doubleToLongBits(b[bFromIndex + i]))
                {
                    return i;
                }
            }
        }

        return -1;
    }

    public static int vectorizedMismatch(Object a, long aOffset, Object b, long bOffset,
                                         int length, int log2ArrayIndexScale)
    {
        // assert a.getClass().isArray();
        // assert b.getClass().isArray();
        // assert 0 <= length <= sizeOf(a)
        // assert 0 <= length <= sizeOf(b)
        // assert 0 <= log2ArrayIndexScale <= 3

        int log2ValuesPerWidth = LOG2_ARRAY_LONG_INDEX_SCALE - log2ArrayIndexScale;
        int wi = 0;
        for (; wi < length >> log2ValuesPerWidth; wi++)
        {
            long bi = ((long) wi) << LOG2_ARRAY_LONG_INDEX_SCALE;
            long av = getLongUnaligned(a, aOffset + bi);
            long bv = getLongUnaligned(b, bOffset + bi);
            if (av != bv)
            {
                long x = av ^ bv;
                int o = nativeIsLittleEndian
                        ? Long.numberOfTrailingZeros(x) >> (LOG2_BYTE_BIT_SIZE + log2ArrayIndexScale)
                        : Long.numberOfLeadingZeros(x) >> (LOG2_BYTE_BIT_SIZE + log2ArrayIndexScale);
                return (wi << log2ValuesPerWidth) + o;
            }
        }

        // Calculate the tail of remaining elements to check
        int tail = length - (wi << log2ValuesPerWidth);

        if (log2ArrayIndexScale < LOG2_ARRAY_INT_INDEX_SCALE)
        {
            int wordTail = 1 << (LOG2_ARRAY_INT_INDEX_SCALE - log2ArrayIndexScale);
            // Handle 4 bytes or 2 chars in the tail using int width
            if (tail >= wordTail)
            {
                long bi = ((long) wi) << LOG2_ARRAY_LONG_INDEX_SCALE;
                int av = getIntUnaligned(a, aOffset + bi);
                int bv = getIntUnaligned(b, bOffset + bi);
                if (av != bv)
                {
                    int x = av ^ bv;
                    int o = nativeIsLittleEndian
                            ? Integer.numberOfTrailingZeros(x) >> (LOG2_BYTE_BIT_SIZE + log2ArrayIndexScale)
                            : Integer.numberOfLeadingZeros(x) >> (LOG2_BYTE_BIT_SIZE + log2ArrayIndexScale);
                    return (wi << log2ValuesPerWidth) + o;
                }
                tail -= wordTail;
            }
            return ~tail;
        }
        else
        {
            return ~tail;
        }
    }
}
