package io.pixelsdb.pixels.index.rockset.jni;

import java.nio.ByteBuffer;

public final class RocksetWriteBatch implements AutoCloseable {

    /**
     * Native handle to rocksdb::WriteBatch
     */
    public long nativeHandle;

    public RocksetWriteBatch(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    /**
     * Create a new empty WriteBatch.
     */
    public static RocksetWriteBatch create() {
        long handle = newWriteBatch0();
        if (handle == 0) {
            throw new RuntimeException("Failed to create RocksetWriteBatch");
        }
        return new RocksetWriteBatch(handle);
    }

    public void put(RocksetColumnFamilyHandle var1, ByteBuffer var2, ByteBuffer var3)
    {
        assert var2.isDirect() && var3.isDirect();

        this.putDirect(this.nativeHandle, var2, var2.position(), var2.remaining(), var3, var3.position(), var3.remaining(), var1.nativeHandle);
        var2.position(var2.limit());
        var3.position(var3.limit());
    }

    public void delete(RocksetColumnFamilyHandle var1, byte[] var2){
        this.delete(this.nativeHandle, var2, var2.length, var1.nativeHandle);
    }

    @Override
    public void close() {
        if (nativeHandle != 0) {
            disposeInternalJni(nativeHandle);
            nativeHandle = 0;
        }
    }

    final void putDirect(long var1, ByteBuffer var3, int var4, int var5, ByteBuffer var6, int var7, int var8, long var9) {
        putDirectJni(var1, var3, var4, var5, var6, var7, var8, var9);
    }

    final void delete(long var1, byte[] var3, int var4, long var5){
        deleteJni(var1, var3, var4, var5);
    }

    /* ================= native ================= */

    private static native long newWriteBatch0();
    private static native void disposeInternalJni(long handle);
    private static native void putDirectJni(long var0, ByteBuffer var2, int var3, int var4, ByteBuffer var5, int var6, int var7, long var8);
    private static native void deleteJni(long var0, byte[] var2, int var3, long var4);
}

