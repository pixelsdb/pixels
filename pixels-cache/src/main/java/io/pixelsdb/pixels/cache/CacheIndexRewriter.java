package io.pixelsdb.pixels.cache;

// convert all big-endian to little-endian
// inplace rewrite
public class CacheIndexRewriter {
    private final MemoryMappedFile indexFile;


    static {
        System.loadLibrary("rewriter");
    }

    CacheIndexRewriter(MemoryMappedFile indexFile) {
        this.indexFile = indexFile;
    }

    public native void rewrite();
}
