package cn.edu.ruc.iir.pixels.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * pixels chunk block.
 * each chunk block is sequential in file, and supposed to be read sequentially on disk
 *
 * @author guodong
 */
public class ChunkSeq
{
    private final List<ChunkId> chunks;
    private long offset = 0;
    private long length = 0;

    public ChunkSeq()
    {
        chunks = new ArrayList<>();
    }

    public ChunkSeq(List<ChunkId> sortedChunks,
                    long offset, long length)
    {
        this.chunks = sortedChunks;
        this.offset = offset;
        this.length = length;
    }

    /**
     * Add chunk by the order of chunks' offset
     */
    public boolean addChunk(ChunkId chunk)
    {
        if (length == 0)
        {
            chunks.add(chunk);
            offset = chunk.getOffset();
            length += chunk.getLength();
            return true;
        }
        else
        {
            if (chunk.getOffset() - offset - length == 0)
            {
                chunks.add(chunk);
                length += chunk.getLength();
                return true;
            }
        }
        return false;
    }

    public void setOffset(long offset)
    {
        this.offset = offset;
    }

    public void setLength(long length)
    {
        this.length = length;
    }

    public List<ChunkId> getChunks()
    {
        return chunks;
    }

    public List<ChunkId> getSortedChunks()
    {
        chunks.sort(Comparator.comparingLong(ChunkId::getOffset));
        return chunks;
    }

    public long getOffset()
    {
        return offset;
    }

    public long getLength()
    {
        return length;
    }
}
