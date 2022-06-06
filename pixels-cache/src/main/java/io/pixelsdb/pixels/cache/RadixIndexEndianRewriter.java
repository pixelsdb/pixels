package io.pixelsdb.pixels.cache;

// convert all big-endian to little-endian
// inplace rewrite
public class RadixIndexEndianRewriter {
    private final MemoryMappedFile indexFile;


    static {
        System.loadLibrary("rewriter");
    }

    RadixIndexEndianRewriter(MemoryMappedFile indexFile) {
        this.indexFile = indexFile;
    }

    public native void rewrite();
}
