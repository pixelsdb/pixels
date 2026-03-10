package io.pixelsdb.pixels.index.rockset.jni;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

public abstract class Filter extends RocksetHandle 
{
    protected Filter(long var1) 
    {
        super(var1);
    }

    protected void disposeInternal() 
    {
        this.disposeInternal(this.nativeHandle);
    }

    protected final void disposeInternal(long var1) 
    {
        disposeInternalJni(var1);
    }

    private static native void disposeInternalJni(long var0);
}
