package io.pixelsdb.pixels.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class NativeHashIndexReader implements AutoCloseable, CacheIndexReader {
    static {
        System.loadLibrary("HashIndexReader");
    }
    private static final Logger logger = LogManager.getLogger(NativeHashIndexReader.class);
    private final int kvSize = PixelsCacheKey.SIZE + PixelsCacheIdx.SIZE;
    private final byte[] kv = new byte[kvSize];

    private final int tableSize;

    private final MemoryMappedFile indexFile;
    private final ByteBuffer cacheIdxBuf = ByteBuffer.allocateDirect(16).order(ByteOrder.LITTLE_ENDIAN);
    private final long cacheIdxBufAddr = ((DirectBuffer) this.cacheIdxBuf).address();

    NativeHashIndexReader(MemoryMappedFile indexFile)
    {
        this.indexFile = indexFile;
        this.tableSize = (int) indexFile.getLong(0);
        System.out.println("tableSize=" + tableSize);
        System.out.println("cacheIdxAddr=" + cacheIdxBufAddr);
    }

    @Override
    public PixelsCacheIdx read(PixelsCacheKey key) {
        return nativeSearch(key.blockId, key.rowGroupId, key.columnId);
    }

    @Override
    public void batchRead(PixelsCacheKey[] keys, PixelsCacheIdx[] results) {
        throw new RuntimeException("not implemented yet");
    }

    private native void doNativeSearch(long mmAddress, long mmSize, long blockId, short rowGroupId, short columnId, long cacheIdxBufAddr);

    private PixelsCacheIdx nativeSearch(long blockId, short rowGroupId, short columnId) {
        cacheIdxBuf.position(0);
        doNativeSearch(indexFile.getAddress(), indexFile.getSize(), blockId, rowGroupId, columnId, cacheIdxBufAddr);
        long offset = cacheIdxBuf.getLong();
        int length = cacheIdxBuf.getInt();
        if (offset == -1) {
            return null;
        } else {
            return new PixelsCacheIdx(offset, length);
        }
    }


    @Override
    public void close() throws Exception {
        try
        {
//            logger.info("cache reader unmaps cache/index file");
            indexFile.unmap();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }


}
