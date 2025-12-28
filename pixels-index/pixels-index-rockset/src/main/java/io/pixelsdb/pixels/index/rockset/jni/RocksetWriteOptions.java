package io.pixelsdb.pixels.index.rockset.jni;

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
