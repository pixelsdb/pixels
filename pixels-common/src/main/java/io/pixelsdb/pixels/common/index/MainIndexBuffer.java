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
     * If the number of files in this buffer is over this threshold, index cache is enabled.
     */
    private static final int CACHE_ENABLE_THRESHOLD = 3;
    /**
     * fileId -> {tableRowId -> rowLocation}.
     */
    private final Map<Long, Map<Long, IndexProto.RowLocation>> indexBuffer;
    private final MainIndexCache indexCache;
    private boolean enableCache = false;

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
            if (this.indexBuffer.size() > CACHE_ENABLE_THRESHOLD)
            {
                this.enableCache = true;
            }
            // Issue #1115: use HashMap for better performance and do post-sorting in flush().
            fileBuffer = new HashMap<>();
            fileBuffer.put(rowId, location);
            if (this.enableCache)
            {
                this.indexCache.insert(rowId, location);
            }
            this.indexBuffer.put(location.getFileId(), fileBuffer);
            return true;
        }
        else
        {
            if (!fileBuffer.containsKey(rowId))
            {
                fileBuffer.put(rowId, location);
                if (this.enableCache)
                {
                    this.indexCache.insert(rowId, location);
                }
                return true;
            }
            return false;
        }
    }

    protected IndexProto.RowLocation lookup(long fileId, long rowId)
    {
        Map<Long, IndexProto.RowLocation> fileBuffer = this.indexBuffer.get(fileId);
        if (fileBuffer == null)
        {
            return null;
        }
        IndexProto.RowLocation location = fileBuffer.get(rowId);
        if (location == null && !this.enableCache)
        {
            location = this.indexCache.lookup(rowId);
        }
        return location;
    }

    /**
     * @param rowId the row id of the table
     * @return the buffered or cached row location, or null if not found
     */
    public IndexProto.RowLocation lookup(long rowId)
    {
        IndexProto.RowLocation location = null;
        if (this.enableCache)
        {
            location = this.indexCache.lookup(rowId);
        }
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
        /* Issue #1115: do post-sorting, build a row id array and sorted it in ascending order.
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
    }
}
