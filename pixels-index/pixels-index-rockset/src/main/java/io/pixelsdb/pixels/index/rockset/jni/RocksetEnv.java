package io.pixelsdb.pixels.index.rockset.jni;

public final class RocksetEnv implements AutoCloseable 
{
    private long nativeHandle;

    private RocksetEnv(long handle) 
    {
        this.nativeHandle = handle;
    }

    public static RocksetEnv create(String bucket, String prefix) 
    {
        long h = createCloudFileSystem0(bucket, prefix);
        return new RocksetEnv(h);
    }

    long nativeHandle() 
    {
        return nativeHandle;
    }

    @Override
    public void close() 
    {
        disposeInternalJni(nativeHandle);
        nativeHandle = 0;
    }

    private static native void disposeInternalJni(long handle);
    private static native long createCloudFileSystem0(String bucket, String prefix);
}

