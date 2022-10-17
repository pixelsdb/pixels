package io.pixelsdb.pixels.cache;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class DiskCacheContentReader implements CacheContentReader {
    private final RandomAccessFile content;

    DiskCacheContentReader(String loc) throws IOException {
        this.content = new RandomAccessFile(loc, "r");
        this.content.seek(0);
    }

    @Override
    public ByteBuffer readZeroCopy(PixelsCacheIdx idx) throws IOException {
        throw new IOException("disk content reader dont support zero copy");
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
        content.seek(idx.offset);
        content.readFully(buf, offset, idx.length);
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
