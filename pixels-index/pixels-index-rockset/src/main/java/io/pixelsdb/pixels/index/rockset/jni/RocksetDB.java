/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.index.rockset.jni;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class RocksetDB
{
    private final long nativeHandle;
    protected RocksetDBOptions options;
    private RocksetColumnFamilyHandle defaultColumnFamilyHandle;
    public static final byte[] DEFAULT_COLUMN_FAMILY;
    final List<RocksetColumnFamilyHandle> ownedColumnFamilyHandles = new ArrayList<>();

    static 
    {
        DEFAULT_COLUMN_FAMILY = "default".getBytes(StandardCharsets.UTF_8);
    }

    public RocksetDB(long nativeHandle)
    {
        this.nativeHandle = nativeHandle;
    }

    public long handle()
    {
        return nativeHandle;
    }

    public static RocksetDB open(RocksetEnv env, RocksetDBOptions var0, String var1, List<RocksetColumnFamilyDescriptor> var2, List<RocksetColumnFamilyHandle> var3)
    {
        byte[][] var4 = new byte[var2.size()][];
        long[] var5 = new long[var2.size()];
        int var6 = -1;

        for(int var7 = 0; var7 < var2.size(); ++var7) 
        {
            RocksetColumnFamilyDescriptor var8 = (RocksetColumnFamilyDescriptor)var2.get(var7);
            var4[var7] = var8.getName();
            var5[var7] = var8.getOptions().handle();
            if (Arrays.equals(var8.getName(), DEFAULT_COLUMN_FAMILY)) 
            {
                var6 = var7;
            }
        }

        if (var6 < 0) 
        {
            throw new IllegalArgumentException("You must provide the default column family in your columnFamilyDescriptors");
        } else 
        {
            long[] var11 = open(env.nativeHandle(), var0.nativeHandle, var1, var4, var5);
            RocksetDB var12 = new RocksetDB(var11[0]);
            var12.storeOptionsInstance(var0);

            for(int var9 = 1; var9 < var11.length; ++var9) 
            {
                RocksetColumnFamilyHandle var10 = new RocksetColumnFamilyHandle(var12, var11[var9]);
                var3.add(var10);
            }

            var12.ownedColumnFamilyHandles.addAll(var3);
            var12.storeDefaultColumnFamilyHandle((RocksetColumnFamilyHandle)var3.get(var6));
            return var12;
        }
    }

    public void put(RocksetColumnFamilyHandle var1, RocksetWriteOptions var2, ByteBuffer var3, ByteBuffer var4) throws RuntimeException {
        if (var3.isDirect() && var4.isDirect()) 
        {
            putDirect(this.nativeHandle, var2.nativeHandle, var3, var3.position(), var3.remaining(), var4, var4.position(), var4.remaining(), var1.nativeHandle);
        } 
        else 
        {
            if (var3.isDirect() || var4.isDirect()) 
            {
                throw new RuntimeException("ByteBuffer parameters must all be direct, or must all be indirect");
            }

            assert var3.hasArray();

            assert var4.hasArray();

            put(this.nativeHandle, var2.nativeHandle, var3.array(), var3.arrayOffset() + var3.position(), var3.remaining(), var4.array(), var4.arrayOffset() + var4.position(), var4.remaining(), var1.nativeHandle);
        }

        var3.position(var3.limit());
        var4.position(var4.limit());
    }

    public void write(RocksetWriteOptions var1, RocksetWriteBatch var2) throws RuntimeException 
    {
        write0(this.nativeHandle, var1.nativeHandle, var2.nativeHandle);
    }

    private void storeOptionsInstance(RocksetDBOptions var0) 
    {
        this.options = var0;
    }

    private void storeDefaultColumnFamilyHandle(RocksetColumnFamilyHandle var1) 
    {
        this.defaultColumnFamilyHandle = var1;
    }

    public RocksetIterator newIterator(RocksetColumnFamilyHandle cfHandle, RocksetReadOptions readOptions)
    {
        long iteratorHandle = iterator(
                this.nativeHandle,
                cfHandle.nativeHandle,
                readOptions.nativeHandle
        );
        return new RocksetIterator(this, iteratorHandle);
    }

    public void close()
    {
        for(RocksetColumnFamilyHandle var2 : this.ownedColumnFamilyHandles) 
        {
            var2.close();
        }
        this.ownedColumnFamilyHandles.clear();
        closeDatabase(nativeHandle);
    }

    public boolean isClosed() 
    {
        return this.nativeHandle == 0;
    }

    public RocksetColumnFamilyHandle createColumnFamily(
      final RocksetColumnFamilyDescriptor columnFamilyDescriptor)
      throws Exception 
    {
        final RocksetColumnFamilyHandle columnFamilyHandle = new RocksetColumnFamilyHandle(this,
            createColumnFamily(nativeHandle, columnFamilyDescriptor.getName(),
                columnFamilyDescriptor.getName().length,
                columnFamilyDescriptor.getOptions().handle()));
        ownedColumnFamilyHandles.add(columnFamilyHandle);
        return columnFamilyHandle;
    }

    // public RocksetColumnFamilyHandle createColumnFamily(byte[] name) 
    // {
    //     try 
    //     {
    //         long handle = createColumnFamily0(this.nativeHandle, name);
    //         return new RocksetColumnFamilyHandle(handle);
    //     } 
    //     catch (Exception e) 
    //     {
    //         throw new RuntimeException(e);
    //     }
    // }

    public long getLongProperty(final String property) throws Exception 
    {
        return getLongProperty(null, property);
    }

    public long getLongProperty(
      /* @Nullable */ final RocksetColumnFamilyHandle columnFamilyHandle,
      final String property) throws Exception 
    {
        return getLongProperty(nativeHandle,
            columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle,
            property, property.length());
    }

    private static native void closeDatabase(long var0);
    private static native long[] open(long env_handle, long options_handle, String db_path, byte[][] descriptors, long[] cf_handles);
    public static native List<byte[]> listColumnFamilies0(String dbPath);
    private static native long createColumnFamily(final long handle, final byte[] columnFamilyName,
      final int columnFamilyNamelen, final long columnFamilyOptions) throws Exception;
    private static native void putDirect(long var0, long var2, ByteBuffer var4, int var5, int var6, ByteBuffer var7, int var8, int var9, long var10) throws RuntimeException;
    private static native void put(long var0, long var2, byte[] var4, int var5, int var6, byte[] var7, int var8, int var9, long var10) throws RuntimeException;
    private static native void write0(long var0, long var2, long var4) throws RuntimeException;
    private static native long iterator(long var0, long var2, long var4);
    private static native long getLongProperty(final long nativeHandle, final long cfHandle,
    final String property, final int propertyLength) throws Exception;
}

