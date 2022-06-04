package io.pixelsdb.pixels.cache;

import java.nio.ByteBuffer;

public class CacheReader {
    private final CacheIndexReader indexReader;
    private final MemoryMappedFile content;

    public CacheReader(CacheIndexReader indexReader, MemoryMappedFile content) {
        this.indexReader = indexReader;
        this.content = content;
    }

    public ByteBuffer get(long blockId, short rowGroupId, short columnId)
    {
        PixelsCacheIdx idx = indexReader.read(new PixelsCacheKey(blockId, rowGroupId, columnId));
        return this.content.getDirectByteBuffer(idx.offset, idx.length);
    }
}
