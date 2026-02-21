package io.pixelsdb.pixels.index.rockset.jni;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

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
