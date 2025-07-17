package io.pixelsdb.pixels.common.index;

import io.pixelsdb.pixels.index.IndexProto;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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
}
