package io.pixelsdb.pixels.cache.utils;

import io.pixelsdb.pixels.cache.MemoryMappedFile;

// convert all big-endian to little-endian
// inplace rewrite
public class RadixIndexEndianRewriter {
    private final MemoryMappedFile indexFile;


    static {
        System.loadLibrary("rewriter");
    }

    public RadixIndexEndianRewriter(MemoryMappedFile indexFile) {
        this.indexFile = indexFile;
    }

    public native void rewrite();
}
