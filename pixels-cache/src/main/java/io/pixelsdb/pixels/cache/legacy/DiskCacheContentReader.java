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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class DiskCacheContentReader implements CacheContentReader {
    private final RandomAccessFile content;

    DiskCacheContentReader(String loc) throws IOException {
        this.content = new RandomAccessFile(loc, "r");
        this.content.seek(0);
    }

    @Override
    public ByteBuffer readZeroCopy(PixelsCacheIdx idx) throws IOException {
        throw new IOException("disk content reader dont support zero copy");
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
        content.seek(idx.offset);
        content.readFully(buf, offset, idx.length);
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
