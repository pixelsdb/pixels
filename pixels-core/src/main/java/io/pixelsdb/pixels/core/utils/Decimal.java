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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created at: 08/04/2022
 * Author: hank
 */
public class Decimal implements Comparable<Decimal>
{
    private static final long[] SCALE_FACTOR = {
            1L,
            10L,
            100L,
            1000L,
            10000L,
            100000L,
            1000000L,
            10000000L,
            100000000L,
            1000000000L,
            10000000000L,
            100000000000L,
            1000000000000L,
            10000000000000L,
            100000000000000L,
            1000000000000000L,
            10000000000000000L,
            100000000000000000L,
            1000000000000000000L};

    public long value;
    public final int precision;
    public final int scale;

    public Decimal(long value, int precision, int scale)
    {
        checkArgument(precision > 0 && scale >= 0 & precision >= scale,
                "invalid precision and scale (" + precision + "," + scale + ")");
        this.value = value;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public int compareTo(Decimal o)
    {
        long i0 = this.value/SCALE_FACTOR[this.scale];
        long i1 = o.value/SCALE_FACTOR[o.scale];
        if (i0 != i1)
        {
            return i0 > i1 ? 1 : -1;
        }
        long d0 = this.value%SCALE_FACTOR[this.scale];
        long d1 = o.value%SCALE_FACTOR[o.scale];
        return d0 > d1 ? 1 : (d0 == d1 ? 0 : -1);
    }

    public int compareTo(long value, int precision, int scale)
    {
        long i0 = this.value/SCALE_FACTOR[this.scale];
        long i1 = value/SCALE_FACTOR[scale];
        if (i0 != i1)
        {
            return i0 > i1 ? 1 : -1;
        }
        long d0 = this.value%SCALE_FACTOR[this.scale];
        long d1 = value%SCALE_FACTOR[scale];
        return d0 > d1 ? 1 : (d0 == d1 ? 0 : -1);
    }

    @Override
    public int hashCode()
    {
        return (int) ((this.value >> 32) ^ this.value ^ this.precision ^ this.scale);
    }

    @Override
    public boolean equals(Object obj)
    {
        // For performance considerations, we do not check the type of obj.
        Decimal o = (Decimal) obj;
        return this.value == o.value && this.scale == o.scale;
    }
}
