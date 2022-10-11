package io.pixelsdb.pixels.cache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

// TODO: shall it be autoclosable?
// TODO: what is the endianess of each reader and writer?
public class MmapFileCacheContentReader implements CacheContentReader {
    private final MemoryMappedFile content;

    MmapFileCacheContentReader(MemoryMappedFile content) throws IOException {
        this.content = content;
    }

    @Override
    public ByteBuffer readZeroCopy(PixelsCacheIdx idx) throws IOException {
        return content.getDirectByteBuffer(idx.offset, idx.length);
    }

    @Override
    public void read(PixelsCacheIdx idx, ByteBuffer buf) throws IOException {
        read(idx, buf.array(), 0);
    }

    @Override
    public void read(PixelsCacheIdx idx, byte[] buf) throws IOException {
        read(idx, buf, 0);
    }

    private void read(PixelsCacheIdx idx, byte[] buf, int offset) throws IOException {
        content.getBytes(idx.offset, buf, offset, idx.length);
    }

    @Override
    public void batchRead(PixelsCacheIdx[] idxs, ByteBuffer buf) throws IOException {
        int offset = 0;
        byte[] internal = buf.array();
        for (int i = 0; i < idxs.length; ++i) {
            read(idxs[i], internal, offset);
            offset += idxs[i].length;
        }
    }
}