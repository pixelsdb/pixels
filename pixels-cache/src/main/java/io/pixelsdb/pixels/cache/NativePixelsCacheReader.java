package io.pixelsdb.pixels.cache;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created at: 19-5-11
 * Author: hank
 */
public class NativePixelsCacheReader
{
    private String indexFileLocation;
    private long indexFileSize;
    private String cacheFileLocation;
    private long cacheFileSize;
    private ByteBuffer keyBuffer = ByteBuffer.allocate(PixelsCacheKey.SIZE).order(ByteOrder.BIG_ENDIAN);

    public NativePixelsCacheReader(String indexFileLocation, long indexFileSize,
                                   String cacheFileLocation, long cacheFileSize)
    {
        this.indexFileLocation = indexFileLocation;
        this.indexFileSize = indexFileSize;
        this.cacheFileLocation = cacheFileLocation;
        this.cacheFileSize = cacheFileSize;
    }

    static
    {
        System.loadLibrary("lib_pixels.so");
    }

    public static class Builder
    {
        private String builderIndexFileLocation;
        private long builderIndexFileSize;
        private String builderCacheFileLocation;
        private long builderCacheFileSize;

        private Builder()
        {
        }

        public NativePixelsCacheReader.Builder setCacheFile(String location, long size)
        {
            this.builderCacheFileLocation = location;
            this.builderCacheFileSize = size;
            return this;
        }

        public NativePixelsCacheReader.Builder setIndexFile(String location, long size)
        {
            this.builderIndexFileLocation = location;
            this.builderIndexFileSize = size;
            return this;
        }

        public NativePixelsCacheReader build()
        {
            return new NativePixelsCacheReader(builderIndexFileLocation, builderIndexFileSize,
                    builderCacheFileLocation, builderCacheFileSize);
        }
    }

    public static NativePixelsCacheReader.Builder newBuilder()
    {
        return new NativePixelsCacheReader.Builder();
    }

    public static native byte[] get(long blockId, short rowGroupId, short columnId);

    public PixelsCacheIdx search(long blockId, short rowGroupId, short columnId)
    {
        return new PixelsCacheIdx(sch(blockId, rowGroupId, columnId));
    }

    private static native byte[] sch(long blockId, short rowGroupId, short columnId);
}
