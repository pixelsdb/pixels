package io.pixelsdb.pixels.index.rockset.jni;

public class RocksetReadOptions extends RocksetHandle
{
    private RocksetReadOptions(long handle)
    {
        super(handle);
    }

    public static RocksetReadOptions create()
    {
        return new RocksetReadOptions(nativeCreate());
    }

    @Override
    public void close()
    {
        nativeRelease(nativeHandle);
    }

    private static native long nativeCreate();
    private static native void nativeRelease(long handle);
    public native long setPrefixSameAsStart(boolean isTrue);
}
