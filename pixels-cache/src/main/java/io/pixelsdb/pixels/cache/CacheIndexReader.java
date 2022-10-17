package io.pixelsdb.pixels.cache;

public interface CacheIndexReader {

    PixelsCacheIdx read(PixelsCacheKey key);
    default PixelsCacheIdx read(long blockId, short rowGroupId, short columnId) {
        return read(new PixelsCacheKey(blockId, rowGroupId, columnId));
    }
    default void batchRead(PixelsCacheKey[] keys, PixelsCacheIdx[] results) {
        for (int i = 0; i < keys.length; ++i) {
            results[i] = read(keys[i]);
        }
    }
}
