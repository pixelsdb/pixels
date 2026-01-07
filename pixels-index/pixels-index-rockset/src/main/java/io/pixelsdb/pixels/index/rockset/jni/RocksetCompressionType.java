package io.pixelsdb.pixels.index.rockset.jni;


public enum RocksetCompressionType 
{
    NO_COMPRESSION((byte)0, (String)null, "kNoCompression"),
    SNAPPY_COMPRESSION((byte)1, "snappy", "kSnappyCompression"),
    ZLIB_COMPRESSION((byte)2, "z", "kZlibCompression"),
    BZLIB2_COMPRESSION((byte)3, "bzip2", "kBZip2Compression"),
    LZ4_COMPRESSION((byte)4, "lz4", "kLZ4Compression"),
    LZ4HC_COMPRESSION((byte)5, "lz4hc", "kLZ4HCCompression"),
    XPRESS_COMPRESSION((byte)6, "xpress", "kXpressCompression"),
    ZSTD_COMPRESSION((byte)7, "zstd", "kZSTD"),
    DISABLE_COMPRESSION_OPTION((byte)127, (String)null, "kDisableCompressionOption");

    private final byte value_;
    private final String libraryName_;
    private final String internalName_;

    public static RocksetCompressionType getCompressionType(String var0) 
    {
        if (var0 != null) 
        {
            for(RocksetCompressionType var4 : values()) 
            {
                if (var4.getLibraryName() != null && var4.getLibraryName().equals(var0)) 
                {
                    return var4;
                }
            }
        }

        return NO_COMPRESSION;
    }

    public static RocksetCompressionType getCompressionType(byte var0) 
    {
        for(RocksetCompressionType var4 : values()) 
        {
            if (var4.getValue() == var0) 
            {
                return var4;
            }
        }
        throw new IllegalArgumentException("Illegal value provided for CompressionType.");
    }

    static RocksetCompressionType getFromInternal(String var0) 
    {
        for(RocksetCompressionType var4 : values()) 
        {
            if (var4.internalName_.equals(var0)) 
            {
                return var4;
            }
        }

        throw new IllegalArgumentException("Illegal internalName '" + var0 + " ' provided for CompressionType.");
    }

    public byte getValue() 
    {
        return this.value_;
    }

    public String getLibraryName() 
    {
        return this.libraryName_;
    }

    private RocksetCompressionType(byte var3, String var4, String var5) 
    {
        this.value_ = var3;
        this.libraryName_ = var4;
        this.internalName_ = var5;
    }
}
