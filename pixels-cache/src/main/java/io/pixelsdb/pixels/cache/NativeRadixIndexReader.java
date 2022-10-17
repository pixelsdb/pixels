package io.pixelsdb.pixels.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class NativeRadixIndexReader implements AutoCloseable, CacheIndexReader {

    static {
        System.loadLibrary("RadixIndexReader");
    }
    private static final Logger logger = LogManager.getLogger(NativeRadixIndexReader.class);

    private final MemoryMappedFile indexFile;
    private final ByteBuffer buf = ByteBuffer.allocateDirect(16).order(ByteOrder.LITTLE_ENDIAN);

    public NativeRadixIndexReader(MemoryMappedFile indexFile) {
        this.indexFile = indexFile;
    }

    private native void search(long mmAddress, long mmSize, ByteBuffer retBuf, long blockId, short rowGroupId, short columnId);

    private PixelsCacheIdx search(long blockId, short rowGroupId, short columnId)
    {
        search(indexFile.getAddress(), indexFile.getSize(), buf, blockId, rowGroupId, columnId);
        long offset = buf.getLong(0);
        int length = (int) buf.getLong(8);
        if (offset == -1) {
            return null;
        } else {
            return new PixelsCacheIdx(offset, length);
        }
    }

    @Override
    public PixelsCacheIdx read(PixelsCacheKey key) {
        return search(key.blockId, key.rowGroupId, key.columnId);
    }

    @Override
    public void batchRead(PixelsCacheKey[] keys, PixelsCacheIdx[] results) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void close() throws Exception {

    }
}
