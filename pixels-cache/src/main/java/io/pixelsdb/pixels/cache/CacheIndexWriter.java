package io.pixelsdb.pixels.cache;

public interface CacheIndexWriter {
    void put(PixelsCacheKey cacheKey, PixelsCacheIdx cacheIdx);
    void clear();
    long flush();
}
