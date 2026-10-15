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
    /** Continuous bulk writes retain the same ranges that the backing index persists. */
    private final Map<Long, NavigableMap<Long, RowIdRange>> bufferedRanges = new HashMap<>();
    private final MainIndexCache indexCache;
    private boolean populateCache = false;

    public static final class FlushSnapshot
    {
        private final long fileId;
        private final int entryCount;
        private final List<RowIdRange> rowIdRanges;

        private FlushSnapshot(long fileId, int entryCount, List<RowIdRange> rowIdRanges)
        {
            this.fileId = fileId;
            this.entryCount = entryCount;
            this.rowIdRanges = Collections.unmodifiableList(new ArrayList<>(rowIdRanges));
        }

        public long getFileId()
        {
            return fileId;
        }

        public int getEntryCount()
        {
            return entryCount;
        }

        public List<RowIdRange> getRowIdRanges()
        {
            return rowIdRanges;
        }

        public boolean isEmpty()
        {
            return entryCount == 0;
        }
    }

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
        if (containingRange(location.getFileId(), rowId) != null)
        {
            return false;
        }
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

    /**
     * Insert a non-overlapping continuous mapping. On overlap nothing changes, so callers
     * can fall back to point insertion and retain per-entry duplicate results.
     */
    public boolean insertRange(RowIdRange range)
    {
        long start = range.getRowIdStart(), end = range.getRowIdEnd();
        checkArgument(start >= 0 && end > start && range.getRgRowOffsetStart() >= 0
                && (long) range.getRgRowOffsetEnd() - range.getRgRowOffsetStart() == end - start,
                "Invalid buffered row id range");
        NavigableMap<Long, RowIdRange> ranges = bufferedRanges.get(range.getFileId());
        if (ranges != null)
        {
            Map.Entry<Long, RowIdRange> before = ranges.floorEntry(start);
            Map.Entry<Long, RowIdRange> after = ranges.ceilingEntry(start);
            if ((before != null && before.getValue().getRowIdEnd() > start)
                    || (after != null && after.getKey() < end))
            {
                return false;
            }
        }
        Map<Long, IndexProto.RowLocation> points = indexBuffer.get(range.getFileId());
        if (points != null)
        {
            for (long rowId : points.keySet())
            {
                if (rowId >= start && rowId < end) return false;
            }
        }
        else
        {
            if (indexBuffer.size() > CACHE_POP_ENABLE_THRESHOLD) populateCache = true;
            indexBuffer.put(range.getFileId(), new HashMap<>());
        }
        if (ranges == null)
        {
            ranges = new TreeMap<>();
            bufferedRanges.put(range.getFileId(), ranges);
        }
        Map.Entry<Long, RowIdRange> before = ranges.lowerEntry(start);
        if (before != null && adjacent(before.getValue(), range))
        {
            range = merge(before.getValue(), range);
            ranges.remove(before.getKey());
        }
        Map.Entry<Long, RowIdRange> after = ranges.ceilingEntry(range.getRowIdEnd());
        if (after != null && adjacent(range, after.getValue()))
        {
            range = merge(range, after.getValue());
            ranges.remove(after.getKey());
        }
        ranges.put(range.getRowIdStart(), range);
        return true;
    }

    private RowIdRange containingRange(long fileId, long rowId)
    {
        NavigableMap<Long, RowIdRange> ranges = bufferedRanges.get(fileId);
        if (ranges == null) return null;
        Map.Entry<Long, RowIdRange> floor = ranges.floorEntry(rowId);
        return floor != null && rowId < floor.getValue().getRowIdEnd() ? floor.getValue() : null;
    }

    private IndexProto.RowLocation rangeLocation(long fileId, long rowId)
    {
        RowIdRange range = containingRange(fileId, rowId);
        return range == null ? null : IndexProto.RowLocation.newBuilder().setFileId(fileId)
                .setRgId(range.getRgId()).setRgRowOffset(range.getRgRowOffsetStart()
                        + (int) (rowId - range.getRowIdStart())).build();
    }

    private static boolean adjacent(RowIdRange left, RowIdRange right)
    {
        return left.getRowIdEnd() == right.getRowIdStart() && left.getFileId() == right.getFileId()
                && left.getRgId() == right.getRgId()
                && left.getRgRowOffsetEnd() == right.getRgRowOffsetStart();
    }

    private static RowIdRange merge(RowIdRange left, RowIdRange right)
    {
        return new RowIdRange(left.getRowIdStart(), right.getRowIdEnd(), left.getFileId(),
                left.getRgId(), left.getRgRowOffsetStart(), right.getRgRowOffsetEnd());
    }

    private int entryCount(long fileId)
    {
        long count = indexBuffer.get(fileId).size();
        NavigableMap<Long, RowIdRange> ranges = bufferedRanges.get(fileId);
        if (ranges != null)
        {
            for (RowIdRange range : ranges.values()) count += range.getRowIdEnd() - range.getRowIdStart();
        }
        return Math.toIntExact(count);
    }

    protected IndexProto.RowLocation lookup(long fileId, long rowId) throws MainIndexException
    {
        Map<Long, IndexProto.RowLocation> fileBuffer = this.indexBuffer.get(fileId);
        if (fileBuffer == null)
        {
            return null;
        }
        IndexProto.RowLocation location = fileBuffer.get(rowId);
        if (location == null) location = rangeLocation(fileId, rowId);
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
        IndexProto.RowLocation location = this.indexCache.lookup(rowId);
        if (location == null)
        {
            for (Map.Entry<Long, Map<Long, IndexProto.RowLocation>> entry : this.indexBuffer.entrySet())
            {
                long fileId = entry.getKey();
                location = entry.getValue().get(rowId);
                if (location == null) location = rangeLocation(fileId, rowId);
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
     * Build a stable snapshot of the (row id -> row location) mappings of the given file id.
     * This method must not mutate the buffer or cache; callers should only discard the buffered
     * entries after the snapshot has been durably committed.
     * @param fileId the given file id to flush
     * @return the row id range snapshot to be persisted into the storage
     * @throws MainIndexException
     */
    public FlushSnapshot snapshotForFlush(long fileId) throws MainIndexException
    {
        Map<Long, IndexProto.RowLocation> fileBuffer = this.indexBuffer.get(fileId);
        if (fileBuffer == null)
        {
            return new FlushSnapshot(fileId, 0, Collections.emptyList());
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
                }
                // start constructing a new row id range
                first = false;
                last = true;
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
        if (last)
        {
            currRangeBuilder.setRowIdEnd(prevRowId + 1);
            currRangeBuilder.setRgRowOffsetEnd(prevRgRowOffset + 1);
            ranges.add(currRangeBuilder.build());
        }
        // release the flushed file index buffer
        if(fileBuffer.size() != rowIds.length)
        {
            throw new MainIndexException("FileBuffer changed while building flush snapshot");
        }
        List<RowIdRange> combined = new ArrayList<>(ranges.build());
        NavigableMap<Long, RowIdRange> bulk = bufferedRanges.get(fileId);
        if (bulk != null) combined.addAll(bulk.values());
        Collections.sort(combined);
        List<RowIdRange> compacted = new ArrayList<>();
        for (RowIdRange range : combined)
        {
            int previous = compacted.size() - 1;
            if (previous >= 0 && adjacent(compacted.get(previous), range))
                compacted.set(previous, merge(compacted.get(previous), range));
            else compacted.add(range);
        }
        return new FlushSnapshot(fileId, entryCount(fileId), compacted);
    }

    /**
     * Discard a flush snapshot after the backing store has durably committed it.
     * @param snapshot the committed snapshot
     * @throws MainIndexException if the buffer no longer matches the committed snapshot
     */
    public void discardFlushed(FlushSnapshot snapshot) throws MainIndexException
    {
        if (snapshot.isEmpty())
        {
            return;
        }
        Map<Long, IndexProto.RowLocation> fileBuffer = this.indexBuffer.get(snapshot.getFileId());
        if (fileBuffer == null || entryCount(snapshot.getFileId()) != snapshot.getEntryCount())
        {
            throw new MainIndexException("FileBuffer changed before committed flush discard");
        }
        fileBuffer.clear();
        this.indexBuffer.remove(snapshot.getFileId());
        this.bufferedRanges.remove(snapshot.getFileId());
        if (this.indexBuffer.size() <= CACHE_POP_ENABLE_THRESHOLD)
        {
            this.populateCache = false;
            this.indexCache.evictAllEntries();
        }
    }

    public List<Long> cachedFileIds()
    {
        return new ArrayList<>(this.indexBuffer.keySet());
    }

    @Override
    public void close() throws IOException
    {
        this.indexBuffer.clear();
        this.bufferedRanges.clear();
        this.indexCache.close();
    }
}
