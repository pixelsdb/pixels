/*
 * Copyright 2022 PixelsDB.
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

import java.math.BigInteger;

/**
 * This class is derived from the Integer128 implementation in trino-spi.
 * We modify it to be compatible with jdk8 and add some methods needed by Pixels.
 *
 * @author hank
 * @date 03/07/2022
 */
public class Integer128 implements Comparable<Integer128>
{
    public static final Integer128 MAX_VALUE = Integer128.valueOf(0x7FFF_FFFF_FFFF_FFFFL, 0xFFFF_FFFF_FFFF_FFFFL);
    public static final Integer128 MIN_VALUE = Integer128.valueOf(0x8000_0000_0000_0000L, 0x0000_0000_0000_0000L);
    public static final long MIN_HIGH = 0x8000_0000_0000_0000L;
    public static final long MIN_LOW = 0x0000_0000_0000_0000L;
    public static final long MAX_HIGH = 0x7FFF_FFFF_FFFF_FFFFL;
    public static final long MAX_LOW = 0xFFFF_FFFF_FFFF_FFFFL;
    public static final Integer128 ONE = Integer128.valueOf(0, 1);
    public static final Integer128 ZERO = Integer128.valueOf(0, 0);

    private long high;
    private long low;

    public Integer128(long high, long low)
    {
        this.high = high;
        this.low = low;
    }

    public void update(long high, long low)
    {
        this.high = high;
        this.low = low;
    }

    public long getHigh()
    {
        return high;
    }

    public long getLow()
    {
        return low;
    }

    /**
     * Decode an Integer128 from the two's complement big-endian representation.
     *
     * @param bytes the two's complement big-endian encoding of the number. It must contain at least 1 byte.
     *              It may contain more than 16 bytes if the leading bytes are not significant (either zeros or -1)
     * @throws ArithmeticException if the bytes represent a number outside of the range [-2^127, 2^127 - 1]
     */
    public static Integer128 fromBigEndian(byte[] bytes)
    {
        if (bytes.length >= 16)
        {
            int offset = bytes.length - Long.BYTES;
            long low = fromBigEndianBytes(bytes, offset);

            offset -= Long.BYTES;
            long high = fromBigEndianBytes(bytes, offset);

            for (int i = 0; i < offset; i++)
            {
                if (bytes[i] != (high >> 63))
                {
                    throw new ArithmeticException("Overflow");
                }
            }

            return Integer128.valueOf(high, low);
        }
        else if (bytes.length > 8)
        {
            // read the last 8 bytes into low
            int offset = bytes.length - Long.BYTES;
            long low = fromBigEndianBytes(bytes, offset);

            // At this point, we're guaranteed to have between 9 and 15 bytes available.
            // Read 8 bytes into high, starting at offset 0. There will be some over-read
            // of bytes belonging to low, so adjust by shifting them out
            long high = fromBigEndianBytes(bytes, 0);
            offset -= Long.BYTES;
            high >>= (-offset * Byte.SIZE);

            return Integer128.valueOf(high, low);
        }
        else if (bytes.length == 8)
        {
            long low = fromBigEndianBytes(bytes, 0);
            long high = (low >> 63);

            return Integer128.valueOf(high, low);
        }
        else
        {
            long high = (bytes[0] >> 7);
            long low = high;
            for (byte aByte : bytes)
            {
                low = (low << 8) | (aByte & 0xFF);
            }

            return Integer128.valueOf(high, low);
        }
    }

    public static Integer128 valueOf(long[] value)
    {
        if (value.length != 2)
        {
            throw new IllegalArgumentException("Expected long[2]");
        }

        long high = value[0];
        long low = value[1];
        return valueOf(high, low);
    }

    public static Integer128 valueOf(long high, long low)
    {
        return new Integer128(high, low);
    }

    public static Integer128 valueOf(String value)
    {
        return Integer128.valueOf(new BigInteger(value));
    }

    public static Integer128 valueOf(BigInteger value)
    {
        long low = value.longValue();
        long high;
        try
        {
            high = value.shiftRight(64).longValueExact();
        }
        catch (ArithmeticException e) {
            throw new ArithmeticException("BigInteger out of Integer128 range");
        }

        return new Integer128(high, low);
    }

    public static Integer128 valueOf(long value)
    {
        return new Integer128(value >> 63, value);
    }

    public void add(long high, long low)
    {
        BigInteger res = this.toBigInteger().add(new BigInteger(toBigEndianBytes(high, low)));
        this.low = res.longValue();
        try
        {
            this.high = res.shiftRight(64).longValueExact();
        }
        catch (ArithmeticException e) {
            throw new ArithmeticException("BigInteger out of Integer128 range");
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        Integer128 that = (Integer128) o;
        return high == that.high && low == that.low;
    }

    @Override
    public int hashCode()
    {
        // FNV-1a style hash
        long hash = 0x9E3779B185EBCA87L;
        hash = (hash ^ high) * 0xC2B2AE3D27D4EB4FL;
        hash = (hash ^ low) * 0xC2B2AE3D27D4EB4FL;
        return Long.hashCode(hash);
    }

    @Override
    public int compareTo(Integer128 other)
    {
        return compare(high, low, other.high, other.low);
    }

    public int compareTo(long high, long low)
    {
        return compare(this.high, this.low,high, low);
    }

    @Override
    public String toString()
    {
        return toBigInteger().toString();
    }

    public BigInteger toBigInteger()
    {
        return new BigInteger(toBigEndianBytes(high, low));
    }

    public long toLong()
    {
        return low;
    }

    public long toLongExact()
    {
        if (high != (low >> 63))
        {
            throw new ArithmeticException("Overflow");
        }

        return low;
    }

    public long[] toLongArray()
    {
        return new long[] {high, low};
    }

    public static int compare(long leftHigh, long leftLow, long rightHigh, long rightLow)
    {
        int comparison = Long.compare(leftHigh, rightHigh);
        if (comparison == 0)
        {
            comparison = Long.compareUnsigned(leftLow, rightLow);
        }

        return comparison;
    }

    public boolean isZero()
    {
        return high == 0 && low == 0;
    }

    public boolean isNegative()
    {
        return high < 0;
    }

    public static byte[] toBigEndianBytes(long high, long low)
    {
        byte[] bytes = new byte[16];
        toBigEndianBytes(high, bytes, 0);
        toBigEndianBytes(low, bytes, Long.BYTES);
        return bytes;
    }

    public static void toBigEndianBytes(long value, byte[] bytes, int offset)
    {
        for(int i = 7; i >= 0; --i)
        {
            bytes[offset+i] = (byte)((int)(value & 255L));
            value >>= 8;
        }
    }

    public static long fromBigEndianBytes(byte[] bytes, int offset)
    {
        return ((long)bytes[offset] & 255L) << 56 | ((long)bytes[offset+1] & 255L) << 48 |
                ((long)bytes[offset+2] & 255L) << 40 | ((long)bytes[offset+3] & 255L) << 32 |
                ((long)bytes[offset+4] & 255L) << 24 | ((long)bytes[offset+5] & 255L) << 16 |
                ((long)bytes[offset+6] & 255L) << 8 | (long)bytes[offset+7] & 255L;
    }
}
