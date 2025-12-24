package io.pixelsdb.pixels.index.rockset.jni;


public final class RocksetStatistics implements AutoCloseable
{
    private final long nativeHandle;

    public RocksetStatistics()
    {
        this.nativeHandle = nativeCreate();
    }

    private static native long nativeCreate();

    long handle()
    {
        return nativeHandle;
    }

    @Override
    public native void close();
}
