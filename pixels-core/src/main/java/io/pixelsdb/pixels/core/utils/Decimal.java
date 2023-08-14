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
import static io.pixelsdb.pixels.core.TypeDescription.MAX_SHORT_DECIMAL_PRECISION;

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

    /**
     * For performance considerations, we expose the value.
     * The user is responsible for ensuring that value does not overflow.
     */
    public long value;
    public final int precision;
    public final int scale;

    public Decimal(long value, int precision, int scale)
    {
        checkArgument(precision > 0 && scale >= 0 && precision >= scale && precision <= MAX_SHORT_DECIMAL_PRECISION,
                "invalid precision and scale (" + precision + "," + scale + ")");
        checkArgument(value < SCALE_FACTOR[precision], "overflow");
        this.value = value;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public int compareTo(Decimal o)
    {
        return this.compareTo(o.value, o.precision, o.scale);
    }

    /**
     * For performance considerations, we do not check overflow in this method.
     * The user should ensure that the value does not overflow.
     * @param value the other value
     * @param precision the precision of the other value
     * @param scale the scale of the other value
     * @return
     */
    public int compareTo(long value, int precision, int scale)
    {
        if (this.scale == scale)
        {
            return (this.scale < scale) ? -1 : ((this.scale == scale) ? 0 : 1);
        }
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
