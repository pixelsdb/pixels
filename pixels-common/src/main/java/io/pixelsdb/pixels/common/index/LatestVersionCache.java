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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;
import io.pixelsdb.pixels.index.IndexProto;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class LatestVersionCache
{
    private final Cache<CacheKey, CacheEntry> cache;

    /**
     * A wrapper for {@link IndexProto.IndexKey} that is used as a key in the cache.
     * The {@link #equals(Object)} and {@link #hashCode()} methods are implemented based on
     * the table ID, index ID, and key value, ignoring the timestamp. This allows cache
     * lookups to succeed for the same logical key regardless of the transaction timestamp.
     */
    private static class CacheKey
    {
        private final IndexProto.IndexKey indexKey;

        public CacheKey(IndexProto.IndexKey indexKey)
        {
            this.indexKey = indexKey;
        }

        public IndexProto.IndexKey getIndexKey()
        {
            return indexKey;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey other = (CacheKey) o;
            // Compare based on tableId
            return indexKey.getTableId() == other.indexKey.getTableId() &&
                    indexKey.getIndexId() == other.indexKey.getIndexId() &&
                    Objects.equals(indexKey.getKey(), other.indexKey.getKey());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(indexKey.getTableId(), indexKey.getIndexId(), indexKey.getKey());
        }
    }

    public static class CacheEntry
    {
        final long rowId;
        final long timestamp;

        CacheEntry (long rowId, long timestamp)
        {
            this.rowId = rowId;
            this.timestamp = timestamp;
        }
    }

    public LatestVersionCache(long maximumSize, long expireAfterAccessSeconds)
    {
        this.cache = Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterAccess(expireAfterAccessSeconds, TimeUnit.SECONDS)
                .build();
    }

    public CacheEntry get(IndexProto.IndexKey key)
    {
        return cache.getIfPresent(new CacheKey(key));
    }

    public void put(IndexProto.IndexKey key, long rowId)
    {
        CacheKey cacheKey = new CacheKey(key);
        long newTimestamp = key.getTimestamp();
        cache.asMap().compute(cacheKey, (k, existingEntry) -> {
            if (existingEntry == null || newTimestamp >= existingEntry.timestamp)
            {
                return new CacheEntry(rowId, newTimestamp);
            } else
            {
                return existingEntry;
            }
        });
    }

    public void invalidate(IndexProto.IndexKey key)
    {
        cache.invalidate(new CacheKey(key));
    }
}
