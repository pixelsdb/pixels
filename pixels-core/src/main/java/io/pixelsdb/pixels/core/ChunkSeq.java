/*
 * Copyright 2017-2019 PixelsDB.
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
package io.pixelsdb.pixels.core;

import java.util.ArrayList;
import java.util.List;

/**
 * pixels chunk block.
 * Each chunk block is sequential in file, and supposed to be read sequentially on disk.
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
            offset = chunk.offset;
            length += chunk.length;
            return true;
        }
        else
        {
            if (chunk.offset - offset - length == 0)
            {
                chunks.add(chunk);
                length += chunk.length;
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

    /**
     * <p>Legacy code.</p>
     * This method should be the same as getChunks(),
     * because chunks are ordered before being added into ChunkSeq.
     * @return
     */
    public List<ChunkId> getSortedChunks()
    {
        chunks.sort((o1, o2) -> (
                o1.offset < o2.offset ? -1 :
                        (o1.offset > o2.offset ? 1 : 0)));
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
