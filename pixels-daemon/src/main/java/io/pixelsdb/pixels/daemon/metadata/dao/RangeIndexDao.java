/*
 * Copyright 2024 PixelsDB.
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
 * @create 2024-05-25
 */
public abstract class RangeIndexDao implements Dao<MetadataProto.RangeIndex>
{
    @Override
    public abstract MetadataProto.RangeIndex getById(long id);

    @Override
    public List<MetadataProto.RangeIndex> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    public abstract MetadataProto.RangeIndex getByTableAndSvIds(long tableId, long schemaVersionId);

    public abstract List<MetadataProto.RangeIndex> getAllByTableId(long tableId);

    public boolean save (MetadataProto.RangeIndex rangeIndex)
    {
        if (exists(rangeIndex))
        {
            return update(rangeIndex);
        }
        else
        {
            return insert(rangeIndex) > 0;
        }
    }

    abstract public boolean exists (MetadataProto.RangeIndex path);

    /**
     * Insert the range index into metadata.
     * @param rangeIndex the range index
     * @return the auto-increment id of the inserted range index, <= 0 if insert is failed
     */
    abstract public long insert (MetadataProto.RangeIndex rangeIndex);

    abstract public boolean update (MetadataProto.RangeIndex rangeIndex);

    abstract public boolean deleteById (long id);
}
