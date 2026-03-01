package io.pixelsdb.pixels.index.rockset.jni;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

public class RocksetReadOptions extends RocksetHandle
{
    private RocksetReadOptions(long handle)
    {
        super(handle);
    }

    public static RocksetReadOptions create()
    {
        return new RocksetReadOptions(newReadOptions());
    }

    public RocksetReadOptions setPrefixSameAsStart(boolean var1) 
    {
        setPrefixSameAsStart(this.nativeHandle, var1);
        return this;
    }
    @Override
    public void close()
    {
        disposeInternalJni(nativeHandle);
    }

    private static native long newReadOptions();
    private static native void disposeInternalJni(long var0);
    private native void setPrefixSameAsStart(long var0, boolean isTrue);
}
