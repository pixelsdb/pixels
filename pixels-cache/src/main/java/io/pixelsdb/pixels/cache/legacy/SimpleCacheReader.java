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
package io.pixelsdb.pixels.cache.legacy;

import io.pixelsdb.pixels.cache.PixelsCacheIdx;
import io.pixelsdb.pixels.cache.PixelsCacheKey;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SimpleCacheReader implements CacheReader
{
    private final CacheIndexReader indexReader;
    private final CacheContentReader contentReader;

    public SimpleCacheReader(CacheIndexReader indexReader, CacheContentReader contentReader)
    {
        this.indexReader = indexReader;
        this.contentReader = contentReader;
    }

    public int naiveget(PixelsCacheKey key, byte[] buf, int size) throws IOException
    {

        PixelsCacheIdx cacheIdx = indexReader.read(key);
        if (cacheIdx == null)
        {
            return 0;
        }
        contentReader.read(cacheIdx, buf);
        return size;
    }

    @Override
    public int get(PixelsCacheKey key, byte[] buf, int size) throws IOException
    {
        PixelsCacheIdx cacheIdx = indexReader.read(key);
        if (cacheIdx == null)
        {
            return 0;
        }
        contentReader.read(cacheIdx, buf);
        return cacheIdx.length;
    }

    @Override
    public PixelsCacheIdx search(PixelsCacheKey key)
    {
        return indexReader.read(key);
    }

    @Override
    public ByteBuffer getZeroCopy(PixelsCacheKey key) throws IOException
    {
        PixelsCacheIdx idx = indexReader.read(key);
        return contentReader.readZeroCopy(idx);
    }
}
