package io.pixelsdb.pixels.index.rockset.jni;


public final class RocksetStatistics implements AutoCloseable
{
    private final long nativeHandle;

    public RocksetStatistics()
    {
        this.nativeHandle = newStatisticsInstance();
    }

    private static native long newStatisticsInstance();
    private static native void disposeInternalJni(long var0);

    long handle()
    {
        return nativeHandle;
    }

    @Override
    public void close()
    {
        disposeInternalJni(nativeHandle);
    }
}
