package io.pixelsdb.pixels.index.rockset.jni;


public abstract class RocksetCache
{
    protected final long nativeHandle;

    protected RocksetCache(long nativeHandle)
    {
        this.nativeHandle = nativeHandle;
    }

    long handle()
    {
        return nativeHandle;
    }

    public abstract void close(long var1);
}