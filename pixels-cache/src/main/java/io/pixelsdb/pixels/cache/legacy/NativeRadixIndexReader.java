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
package io.pixelsdb.pixels.cache.legacy;

import io.pixelsdb.pixels.cache.PixelsCacheIdx;
import io.pixelsdb.pixels.cache.PixelsCacheKey;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class NativeRadixIndexReader implements AutoCloseable, CacheIndexReader
{

    private static final Logger logger = LogManager.getLogger(NativeRadixIndexReader.class);

    static
    {
        System.loadLibrary("RadixIndexReader");
    }

    private final MemoryMappedFile indexFile;
    private final ByteBuffer buf = ByteBuffer.allocateDirect(16).order(ByteOrder.LITTLE_ENDIAN);

    public NativeRadixIndexReader(MemoryMappedFile indexFile)
    {
        this.indexFile = indexFile;
    }

    private native void search(long mmAddress, long mmSize, ByteBuffer retBuf, long blockId, short rowGroupId, short columnId);

    private PixelsCacheIdx search(long blockId, short rowGroupId, short columnId)
    {
        search(indexFile.getAddress(), indexFile.getSize(), buf, blockId, rowGroupId, columnId);
        long offset = buf.getLong(0);
        int length = (int) buf.getLong(8);
        if (offset == -1)
        {
            return null;
        } else
        {
            return new PixelsCacheIdx(offset, length);
        }
    }

    @Override
    public PixelsCacheIdx read(PixelsCacheKey key)
    {
        return search(key.blockId, key.rowGroupId, key.columnId);
    }

    @Override
    public void batchRead(PixelsCacheKey[] keys, PixelsCacheIdx[] results)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void close() throws Exception
    {

    }
}
