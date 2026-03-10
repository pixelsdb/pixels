package io.pixelsdb.pixels.index.rockset.jni;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

public final class RocksetDBOptions extends RocksetHandle
{
    private RocksetDBOptions(long handle)
    {
        super(handle);
    }

    public static RocksetDBOptions create()
    {
        return new RocksetDBOptions(newDBOptions());
    }

    public RocksetDBOptions setCreateIfMissing(boolean value)
    {
        nativeSetCreateIfMissing(nativeHandle, value);
        return this;
    }

    public RocksetDBOptions setCreateMissingColumnFamilies(boolean value)
    {
        nativeSetCreateMissingColumnFamilies(nativeHandle, value);
        return this;
    }

    public RocksetDBOptions setMaxBackgroundFlushes(int value)
    {
        nativeSetMaxBackgroundFlushes(nativeHandle, value);
        return this;
    }

    public RocksetDBOptions setMaxBackgroundCompactions(int value)
    {
        nativeSetMaxBackgroundCompactions(nativeHandle, value);
        return this;
    }

    public RocksetDBOptions setMaxSubcompactions(int value)
    {
        nativeSetMaxSubcompactions(nativeHandle, value);
        return this;
    }

    public RocksetDBOptions setMaxOpenFiles(int value)
    {
        nativeSetMaxOpenFiles(nativeHandle, value);
        return this;
    }

    public RocksetDBOptions setStatistics(RocksetStatistics stats)
    {
        nativeSetStatistics(nativeHandle, stats.handle());
        return this;
    }

    public RocksetDBOptions setStatsDumpPeriodSec(int var1)
    {
        nativeSetStatsDumpPeriodSec(nativeHandle, var1);
        return this;
    }

    public RocksetDBOptions setDbLogDir(String var1)
    {
        nativeSetDbLogDir(nativeHandle, var1);
        return this;
    }

    public void close()
    {
        disposeInternalJni(nativeHandle);
    }

    private static native long newDBOptions();
    private static native void disposeInternalJni(long handle);
    private static native void nativeSetCreateIfMissing(long handle, boolean value);
    private static native void nativeSetCreateMissingColumnFamilies(long handle, boolean value);
    private static native void nativeSetMaxBackgroundFlushes(long handle, int value);
    private static native void nativeSetMaxBackgroundCompactions(long handle, int value);
    private static native void nativeSetMaxSubcompactions(long handle, int value);
    private static native void nativeSetMaxOpenFiles(long handle, int value);
    private static native void nativeSetStatistics(long optionsHandle, long statisticsHandle);
    private static native void nativeSetStatsDumpPeriodSec(long optionsHandle, int var);
    private static native void nativeSetDbLogDir(long optionsHandle, String var);
}
