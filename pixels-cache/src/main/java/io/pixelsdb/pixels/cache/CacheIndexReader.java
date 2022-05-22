package io.pixelsdb.pixels.cache;

public interface CacheIndexReader {
    PixelsCacheIdx read(PixelsCacheKey key);
    void batchRead(PixelsCacheKey[] keys, PixelsCacheIdx[] results);
}
