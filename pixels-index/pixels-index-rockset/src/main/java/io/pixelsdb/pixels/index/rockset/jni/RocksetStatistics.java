package io.pixelsdb.pixels.index.rockset.jni;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

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
