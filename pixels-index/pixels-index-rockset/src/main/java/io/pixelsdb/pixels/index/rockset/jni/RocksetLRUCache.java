package io.pixelsdb.pixels.index.rockset.jni;

public final class RocksetLRUCache extends RocksetCache
{
    public RocksetLRUCache(long capacity, int shardBits)
    {
        super(nativeCreate(capacity, shardBits));
    }

    private static native long nativeCreate(long capacity, int shardBits);

    @Override
    public native void close();
}
