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
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This is the index cache for a main index to accelerate main index lookups.
 * It is to be used inside the main index implementations and protected by the concurrency control in the main index,
 * thus it is not necessary to be thread-safe.
 * @author hank
 * @create 2025-10-17
 */
public class MainIndexCache implements Closeable
{
    /**
     * As row ids in a table is generated sequentially, and the hot row ids are usually the recent generated ones, the
     * conflict rate of row ids should not be very high. Hence, one hash bucket should be good to cache 1 million main
     * index entries.
     */
    private final int NUM_BUCKETS;

    private static final int MAX_BUCKET_SIZE = 1024 * 1024;
    /**
     * tableRowId % NUM_BUCKETS -> {tableRowId -> rowLocation}.
     * We use multiple hash maps (buckets) to reduce hash conflicts in a large cache.
     */
    private final List<Map<Long, IndexProto.RowLocation>> entryCacheBuckets;

    /**
     * Caches the ranges of row ids and the corresponding locations. The cache elements are of the {@link RowIdRange}
     * type. However, we use {@link Object} to support looking up single row ids in this range cache.
     * Ranges in this cache may overlap but not duplicate.
     */
    private final TreeSet<Object> rangeCache;

    public MainIndexCache()
    {
        this.NUM_BUCKETS = Integer.parseInt(ConfigFactory.Instance().getProperty("index.main.cache.bucket.num"));
        this.entryCacheBuckets = new ArrayList<>(NUM_BUCKETS);
        for (int i = 0; i < NUM_BUCKETS; ++i)
        {
            this.entryCacheBuckets.add(new LinkedHashMap<>());
        }
        this.rangeCache = new TreeSet<>((o1, o2) -> {
            /* Issue #1150:
             * o1 is the new element to compare with the existing elements in the set.
             * As we only put RowIdRange into the set, we do not need to check the type of o2.
             *
             * Using such a comparator makes range cache lookup simpler and slightly faster as we do not need to create
             * a dummy RowIdRange for every lookup.
             */
            if (o1 instanceof RowIdRange)
            {
                return ((RowIdRange) o1).compareTo(((RowIdRange) o2));
            }
            else
            {
                long rowId = (Long) o1;
                RowIdRange range = (RowIdRange) o2;
                return rowId < range.getRowIdStart() ? -1 : rowId >= range.getRowIdEnd() ? 1 : 0;
            }
        });
    }

    /**
     * Insert a main index entry into this cache. Previous entry with the same row id is replaced with the new location.
     * @param rowId the table row id of the entry
     * @param location the row location of the entry
     */
    public void admit(long rowId, IndexProto.RowLocation location)
    {
        int bucketId = (int) (rowId % NUM_BUCKETS);
        Map<Long, IndexProto.RowLocation> bucket = this.entryCacheBuckets.get(bucketId);
        if (bucket.size() >= MAX_BUCKET_SIZE)
        {
            // evict one entry
            Iterator<Map.Entry<Long, IndexProto.RowLocation>> it = bucket.entrySet().iterator();
            it.next();
            it.remove();
        }
        bucket.put(rowId, location);
    }

    /**
     * @param rowId the row id to delete
     * @return true if the row id is found and deleted
     */
    public boolean evict(long rowId)
    {
        int bucketId = (int) (rowId % NUM_BUCKETS);
        Map<Long, IndexProto.RowLocation> bucket = this.entryCacheBuckets.get(bucketId);
        return bucket.remove(rowId) != null;
    }

    /**
     * Delete all cached entries in the cache. This does not delete the cached row id ranges.
     */
    public void evictAllEntries()
    {
        for (Map<Long, IndexProto.RowLocation> bucket : this.entryCacheBuckets)
        {
            bucket.clear();
        }
    }

    /**
     * Admitting a row id range that is overlapping but not duplicate with existing ranges in this cache is acceptable.
     * @param range the range to admit into the cache.
     */
    public void admitRange(RowIdRange range)
    {
        this.rangeCache.add(range);
    }

    /**
     * @param range the range to delete from the cache
     * @return true if the range is found and deleted
     */
    public boolean evictRange(RowIdRange range)
    {
        final long rowIdStart = range.getRowIdStart();
        final long rowIdEnd = range.getRowIdEnd();
        checkArgument(rowIdStart < rowIdEnd, "invalid range to evict");
        RowIdRange endRange = (RowIdRange) this.rangeCache.floor(rowIdEnd);
        boolean rangesRemoved = false;
        while (endRange != null && endRange.getRowIdEnd() > rowIdStart)
        {
            this.rangeCache.remove(endRange);
            rangesRemoved = true;
            endRange = (RowIdRange) this.rangeCache.floor(endRange.getRowIdEnd());
        }
        return rangesRemoved;
    }

    /**
     * @param rowId the row id of the table
     * @return the row location of the row id, or null if not found
     */
    public IndexProto.RowLocation lookup(long rowId) throws MainIndexException
    {
        int bucketId = (int) (rowId % NUM_BUCKETS);
        Map<Long, IndexProto.RowLocation> bucket = this.entryCacheBuckets.get(bucketId);
        IndexProto.RowLocation location = bucket.get(rowId);
        if (location == null)
        {
            RowIdRange range = (RowIdRange) rangeCache.floor(rowId);
            if (range != null)
            {
                // check if the floor range equals to (covers) the target row id
                if (range.getRowIdStart() <= rowId && rowId < range.getRowIdEnd())
                {
                    int offset = (int) (rowId - range.getRowIdStart());
                    location = IndexProto.RowLocation.newBuilder().setFileId(range.getFileId())
                            .setRgId(range.getRgId()).setRgRowOffset(range.getRgRowOffsetStart() + offset).build();
                }
            }
        }
        return location;
    }

    @Override
    public void close() throws IOException
    {
        for (Map<Long, IndexProto.RowLocation> bucket : this.entryCacheBuckets)
        {
            bucket.clear();
        }
        this.entryCacheBuckets.clear();
        this.rangeCache.clear();
    }
}
