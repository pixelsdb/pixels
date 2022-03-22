/*
 * Copyright 2019 PixelsDB.
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

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * The location of a cached item, i.e., a cached column chunk,
 * in the cache.
 * @author guodong
 */
public class PixelsCacheIdx
{
    static final int SIZE = Long.BYTES + Integer.BYTES;
    public final long offset;
    public final int length;

    public int dramAccessCount;
    public int radixLevel;

    public PixelsCacheIdx(long offset, int length)
    {
        this.offset = offset;
        this.length = length;
    }

    public PixelsCacheIdx(byte[] content)
    {
        ByteBuffer idxBuffer = ByteBuffer.wrap(content);
        this.offset = idxBuffer.getLong();
        this.length = idxBuffer.getInt();
    }

    public void getBytes(ByteBuffer cacheIdxBuffer)
    {
        cacheIdxBuffer.clear();
        cacheIdxBuffer.putLong(offset);
        cacheIdxBuffer.putInt(length);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(offset).append(", ").append(length);
        return sb.toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this)
        {
            return true;
        }
        if (other != null && other instanceof PixelsCacheIdx)
        {
            PixelsCacheIdx o = (PixelsCacheIdx) other;
            return Objects.equals(offset, o.offset) &&
                    Objects.equals(length, o.length);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(offset, length);
    }
}
