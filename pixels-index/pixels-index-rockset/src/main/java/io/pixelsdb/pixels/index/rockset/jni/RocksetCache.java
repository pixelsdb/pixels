package io.pixelsdb.pixels.index.rockset.jni;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

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