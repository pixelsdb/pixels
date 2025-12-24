package io.pixelsdb.pixels.index.rockset.jni;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

public final class RocksetDB
{
    private final long nativeHandle;
    final List<RocksetColumnFamilyHandle> ownedColumnFamilyHandles = new ArrayList<>();
    private RocksetColumnFamilyHandle defaultColumnFamilyHandle;

    public RocksetDB(long nativeHandle)
    {
        this.nativeHandle = nativeHandle;
    }

    public long handle()
    {
        return nativeHandle;
    }

    public static RocksetDB open(RocksetDBOptions var0, String var1, List<RocksetColumnFamilyDescriptor> var2, List<RocksetColumnFamilyHandle> var3)
    {
        byte[][] var4 = new byte[var2.size()][];
        long[] var5 = new long[var2.size()];
        int var6 = -1;

        for(int var7 = 0; var7 < var2.size(); ++var7) {
            RocksetColumnFamilyDescriptor var8 = (RocksetColumnFamilyDescriptor)var2.get(var7);
            var4[var7] = var8.getName();
            var5[var7] = var8.getOptions().handle();
            if (Arrays.equals(var8.getName(), DEFAULT_COLUMN_FAMILY)) {
                var6 = var7;
            }
        }

        if (var6 < 0) {
            throw new IllegalArgumentException("You must provide the default column family in your columnFamilyDescriptors");
        } else {
            long[] var11 = open(var0.nativeHandle, var1, var4, var5);
            RocksetDB var12 = new RocksetDB(var11[0]);
            var12.storeOptionsInstance(var0);

            for(int var9 = 1; var9 < var11.length; ++var9) {
                RocksetColumnFamilyHandle var10 = new RocksetColumnFamilyHandle(var12, var11[var9]);
                var3.add(var10);
            }

            var12.ownedColumnFamilyHandles.addAll(var3);
            var12.storeDefaultColumnFamilyHandle((RocksetColumnFamilyHandle)var3.get(var6));
            return var12;
        }
    }

    private void storeOptionsInstance(RocksetDBOptions var0) {
    }

    private void storeDefaultColumnFamilyHandle(RocksetColumnFamilyHandle var1) {
        this.defaultColumnFamilyHandle = var1;
    }

    public void close()
    {
        nativeClose(nativeHandle);
    }

    private static native void nativeClose(long handle);
    private static native long[] open(long var0, String var2, byte[][] var3, long[] var4);

    public boolean isClosed() {
        return this.nativeHandle == 0;
    }
}

