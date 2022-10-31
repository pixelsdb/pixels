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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class HashIndexWriter implements CacheIndexWriter {
    private final static Logger logger = LogManager.getLogger(HashIndexWriter.class);
    private long currentIndexOffset = PixelsCacheUtil.INDEX_RADIX_OFFSET;
    private double loadFactor;
    private final int kvSize = PixelsCacheKey.SIZE + PixelsCacheIdx.SIZE;
    private final long tableSize;
    private int nKeys = 0;
    private final byte[] kv = new byte[kvSize];
    private final ByteBuffer kvBuf = ByteBuffer.wrap(kv).order(ByteOrder.LITTLE_ENDIAN);
    private final ByteBuffer keyBuf = ByteBuffer.allocate(PixelsCacheKey.SIZE).order(ByteOrder.LITTLE_ENDIAN);

    private final MemoryMappedFile out;

    public HashIndexWriter(MemoryMappedFile out) {
        this.out = out;
        this.out.clear();
        this.tableSize = out.getSize() / kvSize;
        logger.debug("tablesize=" + this.tableSize);
        out.setLong(currentIndexOffset, tableSize); // write table size, it can also be derived from the mmap file though
        currentIndexOffset += 8;
    }

    private int hashcode(byte[] bytes) {
        int var1 = 1;

        for(int var3 = 0; var3 < bytes.length; ++var3) {
            var1 = 31 * var1 + bytes[var3];
        }

        return var1;
    }


    @Override
    public void put(PixelsCacheKey cacheKey, PixelsCacheIdx cacheIdx) {
        // construct a little endian key buf, this is good for C-compatibility
        keyBuf.position(0);
        keyBuf.putLong(cacheKey.blockId).putShort(cacheKey.rowGroupId).putShort(cacheKey.columnId);
        int hash = hashcode(keyBuf.array()) & 0x7fffffff;
        long bucket = hash % tableSize;
        long offset = bucket * kvSize;
        out.getBytes(offset + currentIndexOffset, kv, 0, this.kvSize);
        boolean valid = kvBuf.getLong(0) == 0 && kvBuf.getLong(8) == 0 && kvBuf.getLong(16) == 0; // all zero
        for(int i = 1; !valid; ++i) {
            bucket += (long) i * i;
            bucket = bucket % tableSize;
            offset = bucket * kvSize;
            out.getBytes(offset + currentIndexOffset, kv, 0, this.kvSize);
            valid = kvBuf.getLong(0) == 0 && kvBuf.getLong(8) == 0 && kvBuf.getLong(16) == 0;
        }
        logger.trace(cacheKey + " put to bucket " + bucket + " offset " + offset);
        kvBuf.position(0);
        kvBuf.putLong(cacheKey.blockId).putShort(cacheKey.rowGroupId).putShort(cacheKey.columnId)
                .putLong(cacheIdx.offset).putInt(cacheIdx.length);

        // find a valid position, insert into hashFile
        out.setBytes(offset + currentIndexOffset, kv);
        nKeys++;
    }

    @Override
    public void clear() {

    }

    @Override
    public long flush() {
        loadFactor = nKeys / (double) tableSize;
        logger.debug("load factor=" + loadFactor + " keys=" + nKeys + " tableSize=" + tableSize);
        return 0;
    }
}
