package io.pixelsdb.pixels.index.rockset.jni;

import java.util.Objects;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

public class RocksetBloomFilter extends Filter
{ 
    private static final double DEFAULT_BITS_PER_KEY = (double)10.0F;
    private final double bitsPerKey;

    public RocksetBloomFilter() 
    {
        this((double)10.0F);
    }

    public RocksetBloomFilter(double var1) 
    {
        this(createNewBloomFilter(var1), var1);
    }

    RocksetBloomFilter(long var1, double var3) 
    {
        super(var1);
        this.bitsPerKey = var3;
    }

    public RocksetBloomFilter(double var1, boolean var3) 
    {
        this(var1);
    }

    public boolean equals(Object var1) 
    {
        if (this == var1) 
        {
            return true;
        } else if (var1 != null && this.getClass() == var1.getClass()) 
        {
            return this.bitsPerKey == ((RocksetBloomFilter)var1).bitsPerKey;
        } else 
        {
            return false;
        }
    }

    public int hashCode() 
    {
        return Objects.hash(new Object[]{this.bitsPerKey});
    }

    private static native long createNewBloomFilter(double var0);

    @Override
    public void close() {}
}
