package io.pixelsdb.pixels.index.rockset.jni;

public class RocksetColumnFamilyHandle extends RocksetHandle
{
    private final RocksetDB rocksetDB;
    RocksetColumnFamilyHandle(long handle)
    {
        super(handle);
        this.rocksetDB = null;
    }

    RocksetColumnFamilyHandle(RocksetDB rocksetDB, long handle)
    {
        super(handle);
        assert rocksetDB != null;
        this.rocksetDB = rocksetDB;
    }

//    public static RocksetColumnFamilyHandle create()
//    {
//        return new RocksetColumnFamilyHandle(nativeCreate());
//    }

    @Override
    public void close()
    {
        nativeRelease(nativeHandle);
    }

//    private static native long nativeCreate();
    private static native void nativeRelease(long handle);
}
