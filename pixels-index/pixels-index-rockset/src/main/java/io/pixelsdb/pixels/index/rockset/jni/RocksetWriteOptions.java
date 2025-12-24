package io.pixelsdb.pixels.index.rockset.jni;

public class RocksetWriteOptions extends RocksetHandle
{
    private RocksetWriteOptions(long handle)
    {
        super(handle);
    }

    public static RocksetWriteOptions create()
    {
        return new RocksetWriteOptions(nativeCreate());
    }

    @Override
    public void close()
    {
        nativeRelease(nativeHandle);
    }

    private static native long nativeCreate();
    private static native void nativeRelease(long handle);
}
