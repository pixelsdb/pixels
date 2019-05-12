package cn.edu.ruc.iir.pixels.cache;

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

    private native byte[] getFromCache();
}
