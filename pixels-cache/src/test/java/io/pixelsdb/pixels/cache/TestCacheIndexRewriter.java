package io.pixelsdb.pixels.cache;

import org.junit.Test;

public class TestCacheIndexRewriter {
    @Test
    public void traverseRadixTree() {
        // cache.location=/dev/shm/pixels.cache
        // cache.size=102400000
        try {
            MemoryMappedFile index = new MemoryMappedFile("/dev/shm/pixels.index", 102400000);
            CacheIndexRewriter cir = new CacheIndexRewriter(index);
            cir.rewrite();
            index.unmap();

        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }
}
