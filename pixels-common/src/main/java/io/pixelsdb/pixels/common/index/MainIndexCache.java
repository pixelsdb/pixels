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

import io.pixelsdb.pixels.index.IndexProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

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
     * index entries. And 101 buckets should be large enough to cache 101 million entries, which may consume several GBs
     * of memory. 101 is a prime number. It helps avoid imbalance buckets.
     */
    private static final int NUM_BUCKETS = 101;

    private static final int MAX_BUCKET_SIZE = 1024 * 1024 * 1024;
    /**
     * tableRowId % NUM_BUCKETS -> {tableRowId -> rowLocation}.
     * We use multiple hash maps (buckets) to reduce hash conflicts in a large cache.
     */
    private final List<Map<Long, IndexProto.RowLocation>> entryCacheBuckets;

    public MainIndexCache()
    {
        this.entryCacheBuckets = new ArrayList<>(NUM_BUCKETS);
        for (int i = 0; i < NUM_BUCKETS; ++i)
        {
            this.entryCacheBuckets.add(new HashMap<>());
        }
    }

    /**
     * Insert a main index entry into this cache. Previous entry with the same row id is replaced with the new location.
     * @param rowId the table row id of the entry
     * @param location the row location of the entry
     */
    public void insert(long rowId, IndexProto.RowLocation location)
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
     * @param rowId the row id of the table
     * @return the row location of the row id, or null if not found
     */
    public IndexProto.RowLocation lookup(long rowId)
    {
        int bucketId = (int) (rowId % NUM_BUCKETS);
        Map<Long, IndexProto.RowLocation> bucket = this.entryCacheBuckets.get(bucketId);
        return bucket.get(rowId);
    }

    @Override
    public void close() throws IOException
    {
        for (Map<Long, IndexProto.RowLocation> bucket : this.entryCacheBuckets)
        {
            bucket.clear();
        }
        this.entryCacheBuckets.clear();
    }
}
