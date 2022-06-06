package io.pixelsdb.pixels.cache;

import org.junit.Test;

public class TestRadixIndexEndianRewriter {
    @Test
    public void traverseRadixTree() {
        // cache.location=/dev/shm/pixels.cache
        // cache.size=102400000
        try {
            MemoryMappedFile index = new MemoryMappedFile("/dev/shm/pixels.index", 102400000);
            RadixIndexEndianRewriter cir = new RadixIndexEndianRewriter(index);
            cir.rewrite();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }
}
