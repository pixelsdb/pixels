package io.pixelsdb.pixels.cache.utils;

import io.pixelsdb.pixels.cache.MemoryMappedFile;

// convert all big-endian to little-endian
// inplace rewrite!!!
public class RadixIndexEndianRewriter {
    private final MemoryMappedFile indexFile;


    static {
        System.loadLibrary("RadixIndexEndianRewriter");
    }

    public RadixIndexEndianRewriter(MemoryMappedFile indexFile) {
        this.indexFile = indexFile;
    }

    public native void rewrite();

    public static void main(String[] args) {
        try {
            MemoryMappedFile index = new MemoryMappedFile("/dev/shm/pixels.index.little", 102400000);
            RadixIndexEndianRewriter cir = new RadixIndexEndianRewriter(index);
            cir.rewrite();
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
