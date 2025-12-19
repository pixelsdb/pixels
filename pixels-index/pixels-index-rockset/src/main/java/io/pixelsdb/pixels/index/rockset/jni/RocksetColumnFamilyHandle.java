package io.pixelsdb.pixels.index.rockset.jni;

public class RocksetColumnFamilyHandle extends RocksetHandle
{
    private RocksetColumnFamilyHandle(long handle)
    {
        super(handle);
    }

    public static RocksetColumnFamilyHandle create()
    {
        return new RocksetColumnFamilyHandle(nativeCreate());
    }

    @Override
    public void close()
    {
        nativeRelease(nativeHandle);
    }

    private static native long nativeCreate();
    private static native void nativeRelease(long handle);
}
