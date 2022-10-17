package io.pixelsdb.pixels.cache;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface CacheContentReader {
    // caller should guarantee that buf.length is enough to hold the CacheIndex
    void read(PixelsCacheIdx idx, byte[] buf) throws IOException;

    ByteBuffer readZeroCopy(PixelsCacheIdx idx) throws IOException;
    default void read(PixelsCacheIdx idx, ByteBuffer buf) throws IOException {
        read(idx, buf.array());
    }

    // caller should guarantee that buf.length is enough to hold all CacheIndexes
    void batchRead(PixelsCacheIdx[] idxs, ByteBuffer buf) throws IOException;
}
