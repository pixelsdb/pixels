package io.pixelsdb.pixels.index.rockset.jni;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

public class RocksetWriteOptions extends RocksetHandle
{
    private RocksetWriteOptions(long handle)
    {
        super(handle);
    }

    public static RocksetWriteOptions create()
    {
        return new RocksetWriteOptions(newWriteOptions());
    }

    @Override
    public void close()
    {
        disposeInternalJni(nativeHandle);
    }

    private static native long newWriteOptions();
    private static native void disposeInternalJni(long handle);
}
