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

/**
 * CompareTo method of this class only compares the offset in cache file.
 * Created at: 2020/9/6
 * Author: hank
 */
public class PixelsCacheKeyIdx implements Comparable<PixelsCacheKeyIdx>
{
    public PixelsCacheIdx idx;
    public long blockId;
    public short rowGroupId;
    public short columnId;

    public PixelsCacheKeyIdx(PixelsCacheIdx idx, long blockId, short rowGroupId, short columnId)
    {
        this.idx = idx;
        this.blockId = blockId;
        this.rowGroupId = rowGroupId;
        this.columnId = columnId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PixelsCacheKeyIdx that = (PixelsCacheKeyIdx) o;
        return Objects.equals(idx, that.idx) &&
                this.blockId == that.blockId &&
                this.rowGroupId == that.rowGroupId &&
                this.columnId == that.columnId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(idx, blockId, rowGroupId, columnId);
    }

    /**
     * Compares this cache index with the specified cache index for order.  Returns a
     * negative integer, zero, or a positive integer as this object is less
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
    public int compareTo(PixelsCacheKeyIdx o)
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
