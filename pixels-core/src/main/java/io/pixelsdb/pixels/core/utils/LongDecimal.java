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

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.core.TypeDescription.LONG_MAX_PRECISION;

/**
 * @date 03/07/2022
 * @author hank
 */
public class LongDecimal implements Comparable<LongDecimal>
{
    private static final BigInteger[] SCALE_FACTOR = {
            BigInteger.TEN.pow(0),
            BigInteger.TEN.pow(1),
            BigInteger.TEN.pow(2),
            BigInteger.TEN.pow(3),
            BigInteger.TEN.pow(4),
            BigInteger.TEN.pow(5),
            BigInteger.TEN.pow(6),
            BigInteger.TEN.pow(7),
            BigInteger.TEN.pow(8),
            BigInteger.TEN.pow(9),
            BigInteger.TEN.pow(10),
            BigInteger.TEN.pow(11),
            BigInteger.TEN.pow(12),
            BigInteger.TEN.pow(13),
            BigInteger.TEN.pow(14),
            BigInteger.TEN.pow(15),
            BigInteger.TEN.pow(16),
            BigInteger.TEN.pow(17),
            BigInteger.TEN.pow(18),
            BigInteger.TEN.pow(19),
            BigInteger.TEN.pow(20),
            BigInteger.TEN.pow(21),
            BigInteger.TEN.pow(22),
            BigInteger.TEN.pow(23),
            BigInteger.TEN.pow(24),
            BigInteger.TEN.pow(25),
            BigInteger.TEN.pow(26),
            BigInteger.TEN.pow(27),
            BigInteger.TEN.pow(28),
            BigInteger.TEN.pow(29),
            BigInteger.TEN.pow(30),
            BigInteger.TEN.pow(31),
            BigInteger.TEN.pow(32),
            BigInteger.TEN.pow(33),
            BigInteger.TEN.pow(34),
            BigInteger.TEN.pow(35),
            BigInteger.TEN.pow(36),
            BigInteger.TEN.pow(37),
            BigInteger.TEN.pow(38)
    };

    public Integer128 value;
    public final int precision;
    public final int scale;

    public LongDecimal(Integer128 value, int precision, int scale)
    {
        checkArgument(precision > 0 && scale >= 0 && precision >= scale && precision <= LONG_MAX_PRECISION,
                "invalid precision and scale (" + precision + "," + scale + ")");
        this.value = value;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public int compareTo(LongDecimal o)
    {
        if (this.scale == o.scale)
        {
            return this.value.compareTo(o.value);
        }

        BigInteger v0 = this.value.toBigInteger();
        BigInteger v1 = o.value.toBigInteger();
        BigInteger i0 = v0.divide(SCALE_FACTOR[this.scale]);
        BigInteger i1 = v1.divide(SCALE_FACTOR[o.scale]);
        if (i0.compareTo(i1) != 0)
        {
            return i0.compareTo(i1);
        }
        BigInteger d0 = v0.mod(SCALE_FACTOR[this.scale]);
        BigInteger d1 = v1.mod(SCALE_FACTOR[o.scale]);
        return d0.compareTo(d1);
    }

    @Override
    public int hashCode()
    {
        return this.value.hashCode() ^ this.precision ^ this.scale;
    }

    @Override
    public boolean equals(Object obj)
    {
        // For performance considerations, we do not check the type of obj.
        LongDecimal o = (LongDecimal) obj;
        return this.value.equals(o.value) && this.scale == o.scale;
    }
}
