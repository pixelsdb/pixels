package io.pixelsdb.pixels.index.rockset.jni;


abstract class RocksetHandle
{
    protected final long nativeHandle;

    protected RocksetHandle(long nativeHandle)
    {
        this.nativeHandle = nativeHandle;
    }

    public long handle()
    {
        return nativeHandle;
    }

    public abstract void close();
}
