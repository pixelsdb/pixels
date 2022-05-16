package io.pixelsdb.pixels.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
//import java.nio.
import java.nio.ByteOrder;
import java.util.Arrays;

public class HashIndexReader implements AutoCloseable {
    static {
        System.loadLibrary("HashIndexReader");
    }
    private static final Logger logger = LogManager.getLogger(PixelsCacheReader.class);
    private final byte[] key = new byte[PixelsCacheKey.SIZE];
    private final ByteBuffer keyBuf = ByteBuffer.wrap(key).order(ByteOrder.LITTLE_ENDIAN);
    private final int kvSize = PixelsCacheKey.SIZE + PixelsCacheIdx.SIZE;
    private final int HEADER_OFFSET = 8;
    private final byte[] kv = new byte[kvSize];
    private final ByteBuffer kvBuf = ByteBuffer.wrap(kv).order(ByteOrder.LITTLE_ENDIAN);

    private final int tableSize;
    // private static CacheLogger cacheLogger = new CacheLogger();

    private final MemoryMappedFile indexFile;
    private final ByteBuffer cacheIdxBuf = ByteBuffer.allocateDirect(16).order(ByteOrder.LITTLE_ENDIAN);
    private final long cacheIdxBufAddr = ((DirectBuffer) this.cacheIdxBuf).address();

    HashIndexReader(MemoryMappedFile indexFile)
    {
        this.indexFile = indexFile;
        this.tableSize = (int) indexFile.getLong(0);
        System.out.println("tableSize=" + tableSize);
        System.out.println("cacheIdxAddr=" + cacheIdxBufAddr);
    }

    private int hashcode(byte[] bytes) {
        int var1 = 1;

        for(int var3 = 0; var3 < bytes.length; ++var3) {
            var1 = 31 * var1 + bytes[var3];
        }

        return var1;
    }

    private native void doNativeSearch(long mmAddress, long mmSize, long blockId, short rowGroupId, short columnId, long cacheIdxBufAddr);

    public PixelsCacheIdx nativeSearch(long blockId, short rowGroupId, short columnId) {
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

    /**
     * This interface is only used by TESTS, DO NOT USE.
     * It will be removed soon!
     */
    // TODO: cache miss handle
    public PixelsCacheIdx search(long blockId, short rowGroupId, short columnId)
    {
        keyBuf.position(0);
        keyBuf.putLong(blockId).putShort(rowGroupId).putShort(columnId);

        int hash = hashcode(keyBuf.array()) & 0x7fffffff;
//        System.out.println("hash=" + hash);
        int bucket = hash & (tableSize - 1); // initial bucket
        int offset = bucket * kvSize;
        indexFile.getBytes(offset + HEADER_OFFSET, kv, 0, this.kvSize);
        kvBuf.position(0);
        boolean valid = keyBuf.position(0).equals(kvBuf.slice().position(0).limit(PixelsCacheKey.SIZE));

        for(int i = 1; !valid; ++i) {
            bucket += i * i;
//                bucket = bucket % tableSize;
            bucket &= tableSize - 1;
            offset = bucket * kvSize;
            indexFile.getBytes(offset + HEADER_OFFSET, kv, 0, this.kvSize);
            // check if key matches
            if (kvBuf.getLong(0) == 0 && kvBuf.getLong(8) == 0 && kvBuf.getLong(16) == 0) {
                System.out.printf("cache miss! blk=%d, rg=%d, col=%d, probe_i=%d, bucket=%d, offset=%d\n", blockId, rowGroupId, columnId, i, bucket, offset);
                return null;
            } // all zero

            valid = keyBuf.position(0).equals(kvBuf.slice().position(0).limit(PixelsCacheKey.SIZE));
//            System.out.println(bucket + " " + offset + " " + i + " " + valid + " " + keyBuf.getLong(0) + " " + kvBuf.getLong(0));
        }

        kvBuf.position(PixelsCacheKey.SIZE);
        // TODO: mind the endianness
        // FIXME: the endianness is reverted!
        return new PixelsCacheIdx(kvBuf.getLong(), kvBuf.getInt());
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
