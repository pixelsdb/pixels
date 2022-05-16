package io.pixelsdb.pixels.cache;

import org.junit.Test;

public class TestHashIndexReader {

    @Test
    public void testConstructor() {
        try {
            MemoryMappedFile indexFile = new MemoryMappedFile("/dev/shm/pixels.hash-index", 102400000);
            HashIndexReader reader = new HashIndexReader(indexFile);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
