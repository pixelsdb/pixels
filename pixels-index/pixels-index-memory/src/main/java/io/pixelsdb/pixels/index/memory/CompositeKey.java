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
package io.pixelsdb.pixels.index.memory;

import com.google.protobuf.ByteString;

import java.util.Objects;

/**
 * @author hank
 * @create 2025-10-12
 */
public class CompositeKey
{
    private final long indexId;
    private final ByteString key;

    public CompositeKey(long indexId, ByteString key)
    {
        this.indexId = indexId;
        this.key = key;
    }

    public long getIndexId()
    {
        return indexId;
    }

    public ByteString getKey()
    {
        return key;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexId, key);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!(obj instanceof CompositeKey))
        {
            return false;
        }
        CompositeKey that = (CompositeKey) obj;
        return this.indexId == that.indexId && Objects.equals(this.key, that.key);
    }
}
