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
    @Test
    public void testNativeSearch() {
        try {
            MemoryMappedFile indexFile = new MemoryMappedFile("/dev/shm/pixels.hash-index", 102400000);
            HashIndexReader reader = new HashIndexReader(indexFile);
            // blk=1073747693, rg=12, col=518
            System.out.println(reader.search(1073747693L, (short) 12, (short) 518));
            System.out.println(reader.nativeSearch(1073747693L, (short) 12, (short) 518));
            System.out.println(reader.nativeSearch(1073747647L, (short) 27, (short) 694));
            System.out.println(reader.nativeSearch(1073747600L, (short) 10, (short) 1013));


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
