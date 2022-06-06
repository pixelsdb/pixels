package io.pixelsdb.pixels.cache;

import org.junit.Test;

public class TestRadixTreeDumper {

    @Test
    public void traverseRadixTree() {
        // cache.location=/dev/shm/pixels.cache
        // cache.size=102400000
        try {
//            MemoryMappedFile index = new MemoryMappedFile("/dev/shm/pixels.index.bak", 102400000);
            MemoryMappedFile index = new MemoryMappedFile("/dev/shm/pixels.index", 102400000);

            RadixTreeDumper cis = new RadixTreeDumper(index);
            cis.traverse();
            cis._traverse();

        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
