package io.pixelsdb.pixels.cache;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SimpleCacheReader implements CacheReader {
    private final CacheIndexReader indexReader;
    private final CacheContentReader contentReader;

    public SimpleCacheReader(CacheIndexReader indexReader, CacheContentReader contentReader) {
        this.indexReader = indexReader;
        this.contentReader = contentReader;
    }

    public int naiveget(PixelsCacheKey key, byte[] buf, int size) throws IOException {

        PixelsCacheIdx cacheIdx = indexReader.read(key);
        if (cacheIdx == null) {
            return 0;
        }
        contentReader.read(cacheIdx, buf);
        return size;
    }

    @Override
    public int get(PixelsCacheKey key, byte[] buf, int size) throws IOException {
        PixelsCacheIdx cacheIdx = indexReader.read(key);
        if (cacheIdx == null) {
            return 0;
        }
        contentReader.read(cacheIdx, buf);
        return cacheIdx.length;
    }

    @Override
    public PixelsCacheIdx search(PixelsCacheKey key) {
        return indexReader.read(key);
    }

    @Override
    public ByteBuffer getZeroCopy(PixelsCacheKey key) throws IOException {
        PixelsCacheIdx idx = indexReader.read(key);
        return contentReader.readZeroCopy(idx);
    }
}
