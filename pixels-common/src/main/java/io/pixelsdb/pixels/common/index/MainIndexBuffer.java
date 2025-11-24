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

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.index.IndexProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This is the index buffer for a main index to accelerate main index writes.
 * It is to be used inside the main index implementations and protected by the concurrency control in the main index,
 * thus it is not necessary to be thread-safe.
 * @author hank
 * @create 2025-07-17
 */
public class MainIndexBuffer implements Closeable
{
    /**
     * Issue #1150:
     * If the number of files in this buffer is over this threshold, synchronous cache population is enabled.
     * 6-8 are tested to be good settings. This threshold avoids redundant cache population when the number of files
     * is small (i.e., the buffer can be used as a cache and provide good lookup performance).
     */
    private static final int CACHE_POP_ENABLE_THRESHOLD = 6;
    /**
     * fileId -> {tableRowId -> rowLocation}.
     */
    private final Map<Long, Map<Long, IndexProto.RowLocation>> indexBuffer;
    private final MainIndexCache indexCache;
    private boolean populateCache = false;

    /**
     * Create a main index buffer and bind the main index cache to it.
     * Entries put into this buffer will also be put into the cache.
     * @param indexCache the main index cache to bind
     */
    public MainIndexBuffer(MainIndexCache indexCache)
    {
        this.indexCache = requireNonNull(indexCache, "indexCache is null");
        this.indexBuffer = new HashMap<>();
    }

    /**
     * Insert a main index entry into this buffer if it does not exist in this buffer.
     * @param rowId the table row id of the entry
     * @param location the row location of the entry
     * @return true of the index entry does not exist and is put successfully in this buffer
     */
    public boolean insert(long rowId, IndexProto.RowLocation location)
    {
        Map<Long, IndexProto.RowLocation> fileBuffer = this.indexBuffer.get(location.getFileId());
        if (fileBuffer == null)
        {
            if (this.indexBuffer.size() > CACHE_POP_ENABLE_THRESHOLD)
            {
                this.populateCache = true;
            }
            // Issue #1115: use HashMap for better performance and do post-sorting in flush().
            fileBuffer = new HashMap<>();
            fileBuffer.put(rowId, location);
            if (this.populateCache)
            {
                this.indexCache.admit(rowId, location);
            }
            this.indexBuffer.put(location.getFileId(), fileBuffer);
            return true;
        }
        else
        {
            if (!fileBuffer.containsKey(rowId))
            {
                fileBuffer.put(rowId, location);
                if (this.populateCache)
                {
                    this.indexCache.admit(rowId, location);
                }
                return true;
            }
            return false;
        }
    }

    protected IndexProto.RowLocation lookup(long fileId, long rowId) throws MainIndexException
    {
        Map<Long, IndexProto.RowLocation> fileBuffer = this.indexBuffer.get(fileId);
        if (fileBuffer == null)
        {
            return null;
        }
        IndexProto.RowLocation location = fileBuffer.get(rowId);
        if (location == null)
        {
            location = this.indexCache.lookup(rowId);
        }
        return location;
    }

    /**
     * @param rowId the row id of the table
     * @return the buffered or cached row location, or null if not found
     */
    public IndexProto.RowLocation lookup(long rowId) throws MainIndexException
    {
        IndexProto.RowLocation location = null;
        location = this.indexCache.lookup(rowId);
        if (location == null)
        {
            for (Map.Entry<Long, Map<Long, IndexProto.RowLocation>> entry : this.indexBuffer.entrySet())
            {
                long fileId = entry.getKey();
                location = entry.getValue().get(rowId);
                if (location != null)
                {
                    checkArgument(fileId == location.getFileId());
                    break;
                }
            }
        }
        return location;
    }

    /**
     * Flush the (row id -> row location) mappings of the given file id into ranges and remove them from the buffer.
     * This method does not evict the main index cache bind to this buffer as the cached entries are not out of date.
     * However, this method may disable synchronous cache population and clear the cache if remaining file ids in the
     * buffer is below or equals to the {@link #CACHE_POP_ENABLE_THRESHOLD}.
     * @param fileId the given file id to flush
     * @return the flushed row id ranges to be persisited into the storage
     * @throws MainIndexException
     */
    public List<RowIdRange> flush(long fileId) throws MainIndexException
    {
        Map<Long, IndexProto.RowLocation> fileBuffer = this.indexBuffer.get(fileId);
        if (fileBuffer == null)
        {
            return null;
        }
        ImmutableList.Builder<RowIdRange> ranges = ImmutableList.builder();
        RowIdRange.Builder currRangeBuilder = new RowIdRange.Builder();
        boolean first = true, last = false;
        long prevRowId = Long.MIN_VALUE;
        int prevRgId = Integer.MIN_VALUE;
        int prevRgRowOffset = Integer.MIN_VALUE;
        /*
         * Issue #1115: do post-sorting, build a row id array and sorted it in ascending order.
         * This consumes less memory and is much faster than building a tree map from fileBuffer.
         */
        Long[] rowIds = new Long[fileBuffer.size()];
        rowIds = fileBuffer.keySet().toArray(rowIds);
        List<Long> sortedRowIds = Arrays.asList(rowIds);
        Collections.sort(sortedRowIds);
        for (long rowId : sortedRowIds)
        {
            IndexProto.RowLocation location = fileBuffer.get(rowId);
            checkArgument(fileId == location.getFileId());
            int rgId = location.getRgId();
            int rgRowOffset = location.getRgRowOffset();
            if (rowId != prevRowId + 1 || rgId != prevRgId || rgRowOffset != prevRgRowOffset + 1)
            {
                // occurs a new row group or a new range in the row group
                if (!first)
                {
                    // finish constructing the current row id range and add it to the ranges
                    currRangeBuilder.setRowIdEnd(prevRowId + 1);
                    currRangeBuilder.setRgRowOffsetEnd(prevRgRowOffset + 1);
                    ranges.add(currRangeBuilder.build());
                    last = true;
                }
                // start constructing a new row id range
                first = false;
                currRangeBuilder.setRowIdStart(rowId);
                currRangeBuilder.setFileId(fileId);
                currRangeBuilder.setRgId(rgId);
                currRangeBuilder.setRgRowOffsetStart(rgRowOffset);
                prevRgId = rgId;
            }
            prevRowId = rowId;
            prevRgRowOffset = rgRowOffset;
        }
        // add the last range
        if (!last)
        {
            currRangeBuilder.setRowIdEnd(prevRowId + 1);
            currRangeBuilder.setRgRowOffsetEnd(prevRgRowOffset + 1);
            ranges.add(currRangeBuilder.build());
        }
        // release the flushed file index buffer
        fileBuffer.clear();
        this.indexBuffer.remove(fileId);
        if (this.indexBuffer.size() <= CACHE_POP_ENABLE_THRESHOLD)
        {
            this.populateCache = false;
            this.indexCache.evictAllEntries();
        }
        return ranges.build();
    }

    public List<Long> cachedFileIds()
    {
        return new ArrayList<>(this.indexBuffer.keySet());
    }

    @Override
    public void close() throws IOException
    {
        this.indexBuffer.clear();
        this.indexCache.close();
    }
}
