/*
 * Copyright 2018-2019 PixelsDB.
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
package io.pixelsdb.pixels.index.rockset;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

public class RocksetIndexStub
{
    // load pixels-index-rockset
    static
    {
        String pixelsHome = System.getenv("PIXELS_HOME");
        if (pixelsHome == null || pixelsHome.isEmpty())
        {
            throw new IllegalStateException("Environment variable PIXELS_HOME is not set");
        }

        String libPath = Paths.get(pixelsHome, "lib/libpixels-index-rockset.so").toString();
        File libFile = new File(libPath);
        if (!libFile.exists())
        {
            throw new IllegalStateException("libpixels-index-rockset.so not found at " + libPath);
        }
        if (!libFile.canRead())
        {
            throw new IllegalStateException("libpixels-index-rockset.so is not readable at " + libPath);
        }
        System.load(libPath);
    }
    // ---------- JNI native bindings ----------
    public native long CreateCloudFileSystem0(
            String bucketName,
            String s3Prefix);
    public native long OpenDBCloud0(
            long cloudEnvPtr,
            String localDbPath,
            String persistentCachePath,
            long persistentCacheSizeGB,
            boolean readOnly);
    public native void DBput0(long dbHandle, byte[] key, byte[] valueOrNull);
    public native byte[] DBget0(long dbHandle, byte[] key);
    public native void DBdelete0(long dbHandle, byte[] key);
    public native void CloseDB0(long dbHandle);

    // iterator
    public native long DBNewIterator0(long dbHandle);
    public native void IteratorSeekForPrev0(long itHandle, byte[] targetKey);
    public native boolean IteratorIsValid0(long itHandle);
    public native byte[] IteratorKey0(long itHandle);
    public native byte[] IteratorValue0(long itHandle);
    public native void IteratorPrev0(long itHandle);
    public native void IteratorClose0(long itHandle);

    // write batch
    public native long WriteBatchCreate0();
    public native void WriteBatchPut0(long wbHandle, byte[] key, byte[] value);
    public native void WriteBatchDelete0(long wbHandle, byte[] key);
    public native boolean DBWrite0(long dbHandle, long wbHandle);
    public native void WriteBatchClear0(long wbHandle);
    public native void WriteBatchDestroy0(long wbHandle);
}