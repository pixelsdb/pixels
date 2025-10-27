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

import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.common.index.LatestVersionCache.CacheEntry;

import java.util.List;

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
        if (isUnique() && cache != null && success)
        {
            cache.put(key, rowId);
        }
        return success;
    }

    @Override
    public boolean putPrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries) throws MainIndexException, SinglePointIndexException
    {
        boolean success = putPrimaryEntriesInternal(entries);
        if (cache != null && success)
        {
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                cache.put(entry.getIndexKey(), entry.getRowId());
            }
        }
        return success;
    }

    @Override
    public boolean putSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException
    {
        boolean success = putSecondaryEntriesInternal(entries);
        if (isUnique() && cache != null && success)
        {
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                cache.put(entry.getIndexKey(), entry.getRowId());
            }
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
    public List<Long> updateSecondaryEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException
    {
        List<Long> previousRowIds = updateSecondaryEntryInternal(key, rowId);
        if (isUnique() && cache != null)
        {
            cache.put(key, rowId);
        }
        return previousRowIds;
    }

    @Override
    public List<Long> updatePrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries) throws SinglePointIndexException
    {
        List<Long> previousRowIds =  updatePrimaryEntriesInternal(entries);
        if (cache != null)
        {
            for (IndexProto.PrimaryIndexEntry entry : entries)
            {
                cache.put(entry.getIndexKey(), entry.getRowId());
            }
        }
        return previousRowIds;
    }

    @Override
    public List<Long> updateSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException
    {
        List<Long> previousRowIds = updateSecondaryEntriesInternal(entries);
        if (isUnique() && cache != null)
        {
            for (IndexProto.SecondaryIndexEntry entry : entries)
            {
                cache.put(entry.getIndexKey(), entry.getRowId());
            }
        }
        return previousRowIds;
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

    @Override
    public List<Long> deleteEntry(IndexProto.IndexKey key) throws SinglePointIndexException
    {
        List<Long> deletedRowIds = deleteEntryInternal(key);
        if (isUnique() && cache != null && !deletedRowIds.isEmpty())
        {
            cache.invalidate(key);
        }
        return deletedRowIds;
    }

    @Override
    public List<Long> deleteEntries(List<IndexProto.IndexKey> keys) throws SinglePointIndexException
    {
        List<Long> deletedRowIds = deleteEntriesInternal(keys);
        if (isUnique() && cache != null && !deletedRowIds.isEmpty())
        {
            for (IndexProto.IndexKey key : keys)
            {
                cache.invalidate(key);
            }
        }
        return deletedRowIds;
    }

    @Override
    public List<Long> purgeEntries(List<IndexProto.IndexKey> indexKeys) throws SinglePointIndexException
    {
        List<Long> purgedRowIds = purgeEntriesInternal(indexKeys);
        if (isUnique() && cache != null && !purgedRowIds.isEmpty())
        {
            for (IndexProto.IndexKey key : indexKeys)
            {
                cache.invalidate(key);
            }
        }
        return purgedRowIds;
    }

    // Abstract methods to be implemented by subclasses
    // These methods perform the actual index operations without caching

    protected abstract long getUniqueRowIdInternal(IndexProto.IndexKey key) throws SinglePointIndexException;
    protected abstract boolean putEntryInternal(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException;
    protected abstract boolean putPrimaryEntriesInternal(List<IndexProto.PrimaryIndexEntry> entries) throws MainIndexException, SinglePointIndexException;
    protected abstract boolean putSecondaryEntriesInternal(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException;
    protected abstract long updatePrimaryEntryInternal(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException;
    protected abstract List<Long> updateSecondaryEntryInternal(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException;
    protected abstract List<Long> updatePrimaryEntriesInternal(List<IndexProto.PrimaryIndexEntry> entries) throws SinglePointIndexException;
    protected abstract List<Long> updateSecondaryEntriesInternal(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException;
    protected abstract long deleteUniqueEntryInternal(IndexProto.IndexKey indexKey) throws SinglePointIndexException;
    protected abstract List<Long> deleteEntryInternal(IndexProto.IndexKey key) throws SinglePointIndexException;
    protected abstract List<Long> deleteEntriesInternal(List<IndexProto.IndexKey> keys) throws SinglePointIndexException;
    protected abstract List<Long> purgeEntriesInternal(List<IndexProto.IndexKey> indexKeys) throws SinglePointIndexException;
}
