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
package io.pixelsdb.pixels.core.predicate;

/**
 * Created at: 08/04/2022
 * Author: hank
 */
public class Decimal implements Comparable<Decimal>
{
    // TODO: implement
    public long value;
    public int precision;
    public int scale;

    public Decimal(long value, int precision, int scale)
    {
        this.value = value;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public int compareTo(Decimal o)
    {
        return 0;
    }

    public int compareTo(long value, int precision, int scale)
    {
        return 0;
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        return super.equals(obj);
    }
}
