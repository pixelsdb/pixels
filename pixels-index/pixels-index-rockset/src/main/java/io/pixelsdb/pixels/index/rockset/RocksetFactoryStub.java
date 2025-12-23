package io.pixelsdb.pixels.index.rockset;

import io.pixelsdb.pixels.index.rockset.jni.RocksetColumnFamilyHandle;
import io.pixelsdb.pixels.index.rockset.jni.RocksetDB;

import java.util.List;

public class RocksetFactoryStub {
    public native List<byte[]> listColumnFamilies0(String dbPath);
    public native RocksetColumnFamilyHandle createColumnFamily(long db, byte[] columnFamilyName) throws Exception;
    public native void closeNativeDatabase(RocksetDB rocksetDB);
}
