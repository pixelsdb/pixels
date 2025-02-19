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
import io.pixelsdb.pixels.index.IndexProto;

import java.io.IOException;
import java.util.List;

/**
 * @author hank
 * @create 2025-02-19
 */
public class RocksDBIndex implements SecondaryIndex
{
    // TODO: implement

    @Override
    public long getUniqueRowId(IndexProto.IndexKey key)
    {
        return 0;
    }

    @Override
    public long[] getRowIds(IndexProto.IndexKey key)
    {
        return new long[0];
    }

    @Override
    public boolean putEntry(IndexProto.IndexKey key, long rowId)
    {
        return false;
    }

    @Override
    public boolean putEntry(Entry entry)
    {
        return false;
    }

    @Override
    public boolean putEntries(List<Entry> entries)
    {
        return false;
    }

    @Override
    public boolean deleteEntry(IndexProto.IndexKey key)
    {
        return false;
    }

    @Override
    public boolean deleteEntries(List<IndexProto.IndexKey> keys)
    {
        return false;
    }

    @Override
    public void close() throws IOException
    {

    }
}
