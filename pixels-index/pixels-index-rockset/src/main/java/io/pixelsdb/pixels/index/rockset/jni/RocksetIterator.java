package io.pixelsdb.pixels.index.rockset.jni;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

import java.nio.ByteBuffer;

public final class RocksetIterator implements AutoCloseable 
{

    /**
     * Native handle to rocksdb::Iterator
     */
    private long nativeHandle;

    /**
     * Owning DB instance (to keep DB alive)
     */
    private final RocksetDB db;

    RocksetIterator(RocksetDB db, long nativeHandle) 
    {
        this.db = db;
        this.nativeHandle = nativeHandle;
    }

    /* ================= cursor ================= */
    public void seek(ByteBuffer var1) 
    {
        ensureValid();
        if (var1.isDirect()) 
        {
            this.seekDirect0(this.nativeHandle, var1, var1.position(), var1.remaining());
        } 
        else 
        {
            this.seekByteArray0(this.nativeHandle, var1.array(), var1.arrayOffset() + var1.position(), var1.remaining());
        }
        var1.position(var1.limit());
    }

    public void next() 
    {
        ensureValid();
        next0(nativeHandle);
    }

    public boolean isValid()
     {
        ensureValid();
        return isValid0(nativeHandle);
    }

    /* ================= data ================= */

    public byte[] key() 
    {
        ensureValid();
        return key0(nativeHandle);
    }

    public byte[] value() 
    {
        ensureValid();
        return value0(nativeHandle);
    }

    /* ================= lifecycle ================= */

    @Override
    public void close() 
    {
        if (nativeHandle != 0) 
        {
            disposeInternalJni(nativeHandle);
            nativeHandle = 0;
        }
    }

    private void ensureValid()
    {
        if (nativeHandle == 0) 
        {
            throw new IllegalStateException("RocksetIterator already closed");
        }
    }

    final void seekDirect0(long var1, ByteBuffer var3, int var4, int var5) 
    {
        seekDirect0Jni(var1, var3, var4, var5);
    }

    final void seekByteArray0(long var1, byte[] var3, int var4, int var5) 
    {
        seekByteArray0Jni(var1, var3, var4, var5);
    }

    /* ================= native ================= */

    private static native void seekDirect0Jni(long var0, ByteBuffer var2, int var3, int var4);
    private static native void seekByteArray0Jni(long var0, byte[] var2, int var3, int var4);
    private static native void next0(long itHandle);
    private static native boolean isValid0(long itHandle);
    private static native byte[] key0(long itHandle);
    private static native byte[] value0(long itHandle);
    private static native void disposeInternalJni(long itHandle);
}
