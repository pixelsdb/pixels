package io.pixelsdb.pixels.cache;

import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NativeDiskCacheContentReader implements CacheContentReader {
    static {
        System.loadLibrary("DiskCacheContentReader");
    }
    @Override
    public void read(PixelsCacheIdx idx, ByteBuffer buf) throws IOException {
        read(idx.offset, idx.length, buf);
    }

    private native void read(long offset, int length, ByteBuffer buf);

    @Override
    public void batchRead(PixelsCacheIdx[] idxs, ByteBuffer buf) throws IOException {
        throw new IOException("Not Implemented");
    }
}
