/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.cache;

import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class NativeHashIndexReader implements AutoCloseable, CacheIndexReader {
    static {
        System.loadLibrary("HashIndexReader");
    }
    private static final Logger logger = LogManager.getLogger(NativeHashIndexReader.class);
    private final int kvSize = PixelsCacheKey.SIZE + PixelsCacheIdx.SIZE;
    private final byte[] kv = new byte[kvSize];

    private final int tableSize;

    private final MemoryMappedFile indexFile;
    private final ByteBuffer cacheIdxBuf = ByteBuffer.allocateDirect(16).order(ByteOrder.LITTLE_ENDIAN);
    private final long cacheIdxBufAddr = ((DirectBuffer) this.cacheIdxBuf).address();

    NativeHashIndexReader(MemoryMappedFile indexFile)
    {
        this.indexFile = indexFile;
        this.tableSize = (int) indexFile.getLong(16);
        System.out.println("tableSize=" + tableSize);
        System.out.println("cacheIdxAddr=" + cacheIdxBufAddr);
    }

    @Override
    public PixelsCacheIdx read(PixelsCacheKey key) {
        return nativeSearch(key.blockId, key.rowGroupId, key.columnId);
    }

    @Override
    public void batchRead(PixelsCacheKey[] keys, PixelsCacheIdx[] results) {
        throw new RuntimeException("not implemented yet");
    }

    private native void doNativeSearch(long mmAddress, long mmSize, long blockId, short rowGroupId, short columnId, long cacheIdxBufAddr);

    private PixelsCacheIdx nativeSearch(long blockId, short rowGroupId, short columnId) {
        cacheIdxBuf.position(0);
        doNativeSearch(indexFile.getAddress(), indexFile.getSize(), blockId, rowGroupId, columnId, cacheIdxBufAddr);
        long offset = cacheIdxBuf.getLong();
        int length = cacheIdxBuf.getInt();
        if (offset == -1) {
            return null;
        } else {
            return new PixelsCacheIdx(offset, length);
        }
    }


    @Override
    public void close() throws Exception {
        try
        {
//            logger.info("cache reader unmaps cache/index file");
            indexFile.unmap();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }


}
