/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.index.rocksdb;

import io.pixelsdb.pixels.common.index.SecondaryIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * @author hank
 * @create 2025-02-09
 */
public class RocksDBIndex implements SecondaryIndex
{
    @Override
    public long getUniqueRowId(ByteBuffer key)
    {
        return 0;
    }

    @Override
    public long[] getRowIds(ByteBuffer key)
    {
        return new long[0];
    }

    @Override
    public boolean putEntry(ByteBuffer key, long rowId)
    {
        return false;
    }

    @Override
    public boolean putEntries(Map<ByteBuffer, Long> entries)
    {
        return false;
    }

    @Override
    public boolean deleteEntry(ByteBuffer key)
    {
        return false;
    }

    @Override
    public boolean deleteEntries(List<ByteBuffer> keys)
    {
        return false;
    }

    @Override
    public void close() throws IOException
    {

    }
}
