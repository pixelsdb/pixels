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

import io.pixelsdb.pixels.cache.mq.MappedBusMessage;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * @author guodong
 * @author hank
 */
public class ColumnletId
        implements MappedBusMessage
{
    public long blockId;
    public short rowGroupId;
    public short columnId;
    long cacheOffset;
    int cacheLength;

    public ColumnletId(short rowGroupId, short columnId)
    {
        this.rowGroupId = rowGroupId;
        this.columnId = columnId;
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
//        mem.putLong(0, blockId);
//        mem.putShort(8, rowGroupId);
//        mem.putShort(12, columnId);
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
        mem.getLong(0);
        mem.getShort(8);
        mem.getShort(12);
    }

    /**
     * Returns the message type.
     *
     * @return the message type
     */
    @Override
    public int type()
    {
        return 0;
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
    public int hashCode()
    {
        return Objects.hash(blockId, rowGroupId, columnId);
    }
}
