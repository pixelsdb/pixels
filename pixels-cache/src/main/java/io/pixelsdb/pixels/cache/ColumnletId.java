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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * ColumnletId can be used as the cache miss message.
 * @author guodong
 * @author hank
 */
public class ColumnletId
        implements Message
{
    private final static int SIZE = 2 * Short.BYTES + Long.BYTES;

    /**
     * Issue 115:
     * Currently, blockId is not assigned and use externally.
     * TODO: assign it with valid value which can be used to lookup the file/object in storage.
     */
    private long blockId = 0;
    public short rowGroupId;
    public short columnId;
    public boolean direct;
    long cacheOffset;
    int cacheLength;

    public ColumnletId(short rowGroupId, short columnId, boolean direct)
    {
        this.rowGroupId = rowGroupId;
        this.columnId = columnId;
        this.direct = direct;
    }

    public ColumnletId()
    {
    }

    /**
     * Writes a message to the bus.
     *
     * @param mem an instance of the memory mapped file
     * @param pos the start of the current record
     */
    @Override
    public void write(MemoryMappedFile mem, long pos)
    {
        mem.setLong(pos, blockId);
        mem.setShort(pos + Long.BYTES, rowGroupId);
        mem.setShort(pos + Long.BYTES + Short.BYTES, columnId);
    }

    /**
     * Reads a message from the bus.
     *
     * @param mem an instance of the memory mapped file
     * @param pos the start of the current record
     */
    @Override
    public void read(MemoryMappedFile mem, long pos)
    {
        blockId = mem.getLong(pos);
        rowGroupId = mem.getShort(pos + Long.BYTES);
        columnId = mem.getShort(pos + Long.BYTES + Short.BYTES);
    }

    /**
     * @return the size of message in bytes.
     */
    @Override
    public int size()
    {
        return SIZE;
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
        ColumnletId other = (ColumnletId) o;
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
    public String print(MemoryMappedFile mem, long pos)
    {
        return toStringHelper(this)
                .add("block id", mem.getLong(pos))
                .add("row group id", mem.getShort(pos + Long.BYTES))
                .add("column id", mem.getShort(pos + Long.BYTES + Short.BYTES))
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(blockId, rowGroupId, columnId);
    }
}
