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
package io.pixelsdb.pixels.daemon.metadata.dao;

import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.List;

/**
 * @author hank
 * @create 2025-02-07
 */
public abstract class SecondaryIndexDao implements Dao<MetadataProto.SecondaryIndex>
{
    @Override
    public abstract MetadataProto.SecondaryIndex getById(long id);

    @Override
    public List<MetadataProto.SecondaryIndex> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    public abstract MetadataProto.SecondaryIndex getByTableId(long tableId);

    public abstract List<MetadataProto.SecondaryIndex> getAllByTableId(long tableId);

    public boolean save (MetadataProto.SecondaryIndex secondaryIndex)
    {
        if (exists(secondaryIndex))
        {
            return update(secondaryIndex);
        }
        else
        {
            return insert(secondaryIndex) > 0;
        }
    }

    abstract public boolean exists (MetadataProto.SecondaryIndex secondaryIndex);

    /**
     * Insert the secondary index into metadata.
     * @param secondaryIndex the secondary index
     * @return the auto-increment id of the inserted secondary index, <= 0 if insert is failed
     */
    abstract public long insert (MetadataProto.SecondaryIndex secondaryIndex);

    abstract public boolean update (MetadataProto.SecondaryIndex secondaryIndex);

    abstract public boolean deleteByTableId(long tableId);
}
