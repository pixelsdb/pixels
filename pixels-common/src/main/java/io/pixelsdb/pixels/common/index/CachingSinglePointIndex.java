/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.common.index;

import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.common.index.LatestVersionCache.CacheEntry;

public abstract class CachingSinglePointIndex implements SinglePointIndex
{
    protected final LatestVersionCache cache;

    public CachingSinglePointIndex()
    {
        ConfigFactory config = ConfigFactory.Instance();
        boolean cacheEnabled = Boolean.parseBoolean(config.getProperty("index.cache.enabled"));

        if (cacheEnabled)
        {
            long capacity = Long.parseLong(config.getProperty("index.cache.capacity"));
            long expireAfterAccessSeconds = Long.parseLong(config.getProperty("index.cache.expiration.seconds"));
            this.cache = new LatestVersionCache(capacity, expireAfterAccessSeconds);
        } else
        {
            this.cache = null;
        }
    }

    @Override
    public long getUniqueRowId(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        if (cache != null)
        {
            CacheEntry cacheEntry = cache.get(key);
            if (cacheEntry != null && cacheEntry.timestamp <= key.getTimestamp())
            {
                return cacheEntry.rowId;
            }
        }

        return getUniqueRowIdInternal(key);
    }

    @Override
    public final boolean putEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        boolean success = putEntryInternal(key, rowId);
        if (cache != null && success)
        {
            cache.put(key, rowId);
        }
        return success;
    }

    @Override
    public long updatePrimaryEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        long previousRowId = updatePrimaryEntryInternal(key, rowId);
        if (cache != null)
        {
            cache.put(key, rowId);
        }
        return previousRowId;
    }

    @Override
    public long deleteUniqueEntry(IndexProto.IndexKey indexKey) throws SinglePointIndexException
    {
        long deleteRowId = deleteUniqueEntryInternal(indexKey);
        if (cache != null && deleteRowId >= 0)
        {
            cache.invalidate(indexKey);
        }
        return deleteRowId;
    }

    // Abstract methods to be implemented by subclasses
    // These methods perform the actual index operations without caching

    protected abstract long getUniqueRowIdInternal(IndexProto.IndexKey key) throws SinglePointIndexException;
    protected abstract boolean putEntryInternal(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException;
    protected abstract long updatePrimaryEntryInternal(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException;
    protected abstract long deleteUniqueEntryInternal(IndexProto.IndexKey indexKey) throws SinglePointIndexException;
}
