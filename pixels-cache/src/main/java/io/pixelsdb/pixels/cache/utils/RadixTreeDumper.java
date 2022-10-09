package io.pixelsdb.pixels.cache.utils;

import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.cache.PixelsCacheUtil;

import java.nio.ByteBuffer;

// dump the little-endian radix tree to a txt file
// note that in current cache(including partitioned radix), we use big-endian radix tree more
// because the radix tree writer is directly adapted from previous java code
// yet, you can convert a big-endian radix index file by RadixIndexEndianRewriter
public class RadixTreeDumper {

    // little endian radix tree file
    private final MemoryMappedFile indexFile;

    static {
        System.loadLibrary("RadixTreeDumper");
    }

    RadixTreeDumper(MemoryMappedFile indexFile) {
        this.indexFile = indexFile;
    }

    // traverse the index
    // TODO: currently the output file name is fixed to dumpedCache.txt in c file
    public native void nativeTraverse();


    public static void main(String[] args) {
        try {
            MemoryMappedFile index = new MemoryMappedFile("/dev/shm/pixels.index.little", 102400000);

            RadixTreeDumper cis = new RadixTreeDumper(index);
            cis.nativeTraverse();

        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
