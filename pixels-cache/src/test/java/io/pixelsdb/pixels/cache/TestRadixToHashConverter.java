package io.pixelsdb.pixels.cache;

import org.junit.Test;
public class TestRadixToHashConverter {

    @Test
    public void testConstructor() {
        try {
            MemoryMappedFile radixFile = new MemoryMappedFile("/dev/shm/pixels.index", 102400000);
            RadixToHashConverter converter = new RadixToHashConverter(radixFile, "/dev/shm/pixels.hash-index", 0.5);

            converter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Test
    public void testConvert() {
        try {
            MemoryMappedFile radixFile = new MemoryMappedFile("/dev/shm/pixels.index", 102400000);
            RadixToHashConverter converter = new RadixToHashConverter(radixFile, "/dev/shm/pixels.hash-index", 0.5);
            converter.rewriteRadixToHash();
            converter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
