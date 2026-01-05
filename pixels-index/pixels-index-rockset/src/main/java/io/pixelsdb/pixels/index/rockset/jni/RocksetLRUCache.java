package io.pixelsdb.pixels.index.rockset.jni;

public final class RocksetLRUCache extends RocksetCache
{
    public RocksetLRUCache(long capacity, int shardBits)
    {
        super(newLRUCache(capacity, shardBits, false, (double)0.0F, (double) 0.0F));
    }

    private static native long newLRUCache(long var0, int var2, boolean var3, double var4, double var6);
    private static native void disposeInternalJni(long var0);

    @Override
    public void close(long var1)
    {
        disposeInternalJni(var1);
    }
}
