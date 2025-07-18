package io.pixelsdb.pixels.common.index;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.index.IndexProto;

import java.util.*;

/**
 * @author hank
 * @create 2025-07-17
 */
public class MainIndexBuffer
{
    /**
     * fileId -> {tableRowId -> rowLocation}.
     */
    private final Map<Long, Map<Long, IndexProto.RowLocation>> indexBuffer;

    protected MainIndexBuffer()
    {
        indexBuffer = new HashMap<>();
    }

    /**
     * Insert a main index entry into this buffer.
     * @param rowId the table row id of the entry
     * @param location the row location of the entry
     * @return the previous row location of the same file, row group, and file id. Or null if this is a new entry
     */
    protected synchronized IndexProto.RowLocation insert(long rowId, IndexProto.RowLocation location)
    {
        Map<Long, IndexProto.RowLocation> fileBuffer = indexBuffer.get(location.getFileId());
        if (fileBuffer == null)
        {
            fileBuffer = new TreeMap<>();
            fileBuffer.put(rowId, location);
            indexBuffer.put(location.getFileId(), fileBuffer);
            return null;
        }
        else
        {
            return fileBuffer.put(rowId, location);
        }
    }

    protected synchronized IndexProto.RowLocation lookup(long fileId, long rowId)
    {
        Map<Long, IndexProto.RowLocation> fileBuffer = indexBuffer.get(fileId);
        if (fileBuffer == null)
        {
            return null;
        }
        return fileBuffer.get(rowId);
    }

    protected synchronized List<RowIdRange> flush (long fileId) throws MainIndexException
    {
        Map<Long, IndexProto.RowLocation> fileBuffer = indexBuffer.get(fileId);
        if (fileBuffer == null)
        {
            return null;
        }
        ImmutableList.Builder<RowIdRange> ranges = ImmutableList.builder();
        RowIdRange currRange = null;
        long prevRowId = Long.MIN_VALUE;
        int prevRgId = Integer.MIN_VALUE;
        int prevRgRowId = Integer.MIN_VALUE;
        for (Map.Entry<Long, IndexProto.RowLocation> entry : fileBuffer.entrySet())
        {
            // file buffer is a tree map, its entries are sorted in ascending order
            long rowId = entry.getKey();
            IndexProto.RowLocation location = entry.getValue();
            if (location.getFileId() != fileId)
            {
                throw new MainIndexException("file index buffer contains invalid fileId: " + location.getFileId());
            }
            int rgId = location.getRgId();
            int rgRowId = location.getRgRowId();
            if (rowId != prevRowId + 1 || rgId != prevRgId || rgRowId != prevRgRowId + 1)
            {
                // occurs a new row group or a new range in the row group
                if (currRange != null)
                {
                    // finish constructing the current row id range and add it to the ranges
                    currRange.setRowIdEnd(prevRowId + 1);
                    currRange.setRgRowIdEnd(prevRgRowId + 1);
                    ranges.add(currRange);
                }
                // start constructing a new row id range
                currRange = new RowIdRange(rowId, fileId, rgId, rgRowId);
                prevRgId = rgId;
            }
            prevRowId = rowId;
            prevRgRowId = rgRowId;
        }
        // release the flushed file index buffer
        fileBuffer.clear();
        indexBuffer.remove(fileId);
        return ranges.build();
    }
}
