package io.pixelsdb.pixels.index.rockset.jni;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

import java.util.Arrays;
import java.util.Objects;

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

    protected final void disposeInternal(long var1) {
        disposeInternalJni(var1);
    }

    private static native void disposeInternalJni(long var0);

    @Override
    public void close()
    {
        this.disposeInternal(this.nativeHandle);
    }
}
