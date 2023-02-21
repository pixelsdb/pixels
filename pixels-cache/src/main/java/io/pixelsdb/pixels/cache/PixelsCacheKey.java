/*
 * Copyright 2019 PixelsDB.
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

import io.pixelsdb.pixels.cache.mq.Message;
import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;

import java.nio.ByteBuffer;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * The key to look up the cache index. It is also the cache miss message
 * that can be written into the cache miss message queue.
 *
 * @author guodong
 * @author hank
 */
public class PixelsCacheKey implements Message
{
    final static int SIZE = 2 * Short.BYTES + Long.BYTES;

    public long blockId;
    public short rowGroupId;
    public short columnId;
    // Big-endian is prefix comparable and efficient for radix-tree.
    // Although big endian is used as the default byte order in ByteBuffer, we still want to make sure.
    // Block id in hdfs-2.7.3 is a sequence number in each block-id pool, not really random.
    // Currently we only support HDFS cluster without NameNode federation, in which there is only one
    // block-id pool.

    public PixelsCacheKey(long blockId, short rowGroupId, short columnId)
    {
        this.blockId = blockId;
        this.rowGroupId = rowGroupId;
        this.columnId = columnId;
    }

    public void getBytes(ByteBuffer keyBuffer)
    {
        keyBuffer.clear();
        keyBuffer.putLong(blockId);
        keyBuffer.putShort(rowGroupId);
        keyBuffer.putShort(columnId);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        PixelsCacheKey other = (PixelsCacheKey) o;
        return Objects.equals(blockId, other.blockId) &&
                Objects.equals(rowGroupId, other.rowGroupId) &&
                Objects.equals(columnId, other.columnId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("block id", blockId)
                .add("row group id", rowGroupId)
                .add("column id", columnId)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(blockId, rowGroupId, columnId);
    }

    /**
     * Return the footprint of this message in the shared memory.
     *
     * @return the footprint in bytes.
     */
    @Override
    public int size()
    {
        return SIZE;
    }

    /**
     * Set the message size. When reading a message from the message queue,
     * this method is called to set the message size for this.read().
     *
     * @param messageSize
     */
    @Override
    public void ensureSize(int messageSize)
    {
        checkArgument(messageSize == SIZE, "incorrect message size");
    }

    /**
     * Read the content of this message from the given pos in sharedMemory.
     *
     * @param mem the shared memory to read this message from.
     * @param pos the byte position in the shared memory.
     */
    @Override
    public void read(MemoryMappedFile mem, long pos)
    {
        blockId = mem.getLong(pos);
        rowGroupId = mem.getShort(pos + Long.BYTES);
        columnId = mem.getShort(pos + Long.BYTES + Short.BYTES);
    }

    /**
     * Write the content of this message into the given pos in sharedMemory.
     *
     * @param mem the shared memory to write this message into.
     * @param pos the byte position in the shared memory.
     */
    @Override
    public void write(MemoryMappedFile mem, long pos)
    {
        mem.setLong(pos, blockId);
        mem.setShort(pos + Long.BYTES, rowGroupId);
        mem.setShort(pos + Long.BYTES + Short.BYTES, columnId);
    }

    /**
     * This method is only used for debugging.
     *
     * @param mem the shared memory to read and print this message.
     * @param pos the byte position in the shared memory.
     * @return the content of the message.
     */
    @Override
    public String print(MemoryMappedFile mem, long pos)
    {
        return toStringHelper(this)
                .add("block id", mem.getLong(pos))
                .add("row group id", mem.getShort(pos + Long.BYTES))
                .add("column id", mem.getShort(pos + Long.BYTES + Short.BYTES))
                .toString();
    }

    public static void getBytes(ByteBuffer keyBuffer, long blockId, short rowGroupId, short columnId)
    {
        keyBuffer.clear();
        keyBuffer.putLong(blockId);
        keyBuffer.putShort(rowGroupId);
        keyBuffer.putShort(columnId);
    }

    public static void getBytes(ByteBuffer keyBuffer, PixelsCacheKey key)
    {
        keyBuffer.clear();
        keyBuffer.putLong(key.blockId);
        keyBuffer.putShort(key.rowGroupId);
        keyBuffer.putShort(key.columnId);
    }
}
