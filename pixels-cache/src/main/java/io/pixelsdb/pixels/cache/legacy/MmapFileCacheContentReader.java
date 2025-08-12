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
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;

import java.io.IOException;
import java.nio.ByteBuffer;

// TODO: shall it be autoclosable?
// TODO: what is the endianess of each reader and writer?
public class MmapFileCacheContentReader implements CacheContentReader {
    private final MemoryMappedFile content;

    MmapFileCacheContentReader(MemoryMappedFile content) throws IOException {
        this.content = content;
    }

    @Override
    public ByteBuffer readZeroCopy(PixelsCacheIdx idx) throws IOException {
        return content.getDirectByteBuffer(idx.offset, idx.length);
    }

    @Override
    public void read(PixelsCacheIdx idx, ByteBuffer buf) throws IOException {
        read(idx, buf.array(), 0);
    }

    @Override
    public void read(PixelsCacheIdx idx, byte[] buf) throws IOException {
        read(idx, buf, 0);
    }

    private void read(PixelsCacheIdx idx, byte[] buf, int offset) throws IOException {
        content.getBytes(idx.offset, buf, offset, idx.length);
    }

    @Override
    public void batchRead(PixelsCacheIdx[] idxs, ByteBuffer buf) throws IOException {
        int offset = 0;
        byte[] internal = buf.array();
        for (int i = 0; i < idxs.length; ++i) {
            read(idxs[i], internal, offset);
            offset += idxs[i].length;
        }
    }
}