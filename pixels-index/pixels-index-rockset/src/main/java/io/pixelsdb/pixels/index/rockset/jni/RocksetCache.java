package io.pixelsdb.pixels.index.rockset.jni;


public abstract class RocksetCache implements AutoCloseable
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

    @Override
    public abstract void close();
}