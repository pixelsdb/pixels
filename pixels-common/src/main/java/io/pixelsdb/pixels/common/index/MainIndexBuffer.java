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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This is the index buffer for a main index.
 * It is to be used inside the main index implementations and protected by the concurrent control in the main index,
 * thus it is not necessary to be thread-safe.
 * @author hank
 * @create 2025-07-17
 */
public class MainIndexBuffer implements Closeable
{
    /**
     * fileId -> {tableRowId -> rowLocation}.
     */
    private final Map<Long, Map<Long, IndexProto.RowLocation>> indexBuffer;

    public MainIndexBuffer()
    {
        indexBuffer = new HashMap<>();
    }

    /**
     * Insert a main index entry into this buffer if it does not exist in this buffer.
     * @param rowId the table row id of the entry
     * @param location the row location of the entry
     * @return true of the index entry does not exist and is put successfully in this buffer
     */
    public boolean insert(long rowId, IndexProto.RowLocation location)
    {
        Map<Long, IndexProto.RowLocation> fileBuffer = indexBuffer.get(location.getFileId());
        if (fileBuffer == null)
        {
            fileBuffer = new TreeMap<>();
            fileBuffer.put(rowId, location);
            indexBuffer.put(location.getFileId(), fileBuffer);
            return true;
        }
        else
        {
            if (!fileBuffer.containsKey(rowId))
            {
                fileBuffer.put(rowId, location);
                return true;
            }
            return false;
        }
    }

    protected IndexProto.RowLocation lookup(long fileId, long rowId)
    {
        Map<Long, IndexProto.RowLocation> fileBuffer = indexBuffer.get(fileId);
        if (fileBuffer == null)
        {
            return null;
        }
        return fileBuffer.get(rowId);
    }

    public IndexProto.RowLocation lookup(long rowId)
    {
        for (Map.Entry<Long, Map<Long, IndexProto.RowLocation>> entry : indexBuffer.entrySet())
        {
            long fileId = entry.getKey();
            IndexProto.RowLocation location = entry.getValue().get(rowId);
            if (location != null)
            {
                checkArgument(fileId == location.getFileId());
                return location;
            }
        }
        return null;
    }

    public List<RowIdRange> flush(long fileId) throws MainIndexException
    {
        Map<Long, IndexProto.RowLocation> fileBuffer = indexBuffer.get(fileId);
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
        for (Map.Entry<Long, IndexProto.RowLocation> entry : fileBuffer.entrySet())
        {
            // file buffer is a tree map, its entries are sorted in ascending order
            long rowId = entry.getKey();
            IndexProto.RowLocation location = entry.getValue();
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
        indexBuffer.remove(fileId);
        return ranges.build();
    }

    public List<Long> cachedFileIds()
    {
        return indexBuffer.keySet().stream().map(id -> id).collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException
    {
        this.indexBuffer.clear();
    }
}
