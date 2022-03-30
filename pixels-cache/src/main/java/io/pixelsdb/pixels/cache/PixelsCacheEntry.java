/*
 * Copyright 2020 PixelsDB.
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
package io.pixelsdb.pixels.cache;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * This is the combination of PixelsCacheKey and PixelsCacheIdx.
 *
 * <p><b>Note: </b>The CompareTo method of this class only compares the offset in cache file.</p>
 *
 * Created at: 2020/9/6
 * @author: hank
 */
public class PixelsCacheEntry implements Comparable<PixelsCacheEntry>
{
    public PixelsCacheKey key;
    public PixelsCacheIdx idx;

    public PixelsCacheEntry(PixelsCacheKey key, PixelsCacheIdx idx)
    {
        this.key = requireNonNull(key, "key is null");
        this.idx = requireNonNull(idx, "idx is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PixelsCacheEntry that = (PixelsCacheEntry) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(idx, that.idx);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(key, idx);
    }

    /**
     * Compares this cache entry with the specified cache entry for sorting.
     * Returns a negative integer, zero, or a positive integer if this object is less
     * than, equal to, or greater than the specified object.
     *
     * <p>
     *     Note that null is smaller than any not-null values.
     * </p>

     * @param o the object to be compared.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     * @throws NullPointerException if the specified object is null
     * @throws ClassCastException   if the specified object's type prevents it
     *                              from being compared to this object.
     */
    @Override
    public int compareTo(PixelsCacheEntry o)
    {
        if (o == null)
        {
            return 1;
        }
        if (this.idx.offset < o.idx.offset)
        {
            return -1;
        }
        else if (this.idx.offset == o.idx.offset)
        {
            return 0;
        }
        else
        {
            return 1;
        }
    }
}
