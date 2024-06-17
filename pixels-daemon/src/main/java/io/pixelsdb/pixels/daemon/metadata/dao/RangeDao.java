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
public abstract class RangeDao implements Dao<MetadataProto.Range>
{
    @Override
    public abstract MetadataProto.Range getById(long id);

    @Override
    public List<MetadataProto.Range> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    public abstract List<MetadataProto.Range> getAllByRangeIndexId(long rangeIndexId);

    public boolean save (MetadataProto.Range range)
    {
        if (exists(range))
        {
            return update(range);
        }
        else
        {
            return insert(range) > 0;
        }
    }

    abstract public boolean exists (MetadataProto.Range range);

    /**
     * Insert the range into metadata.
     * @param range the range
     * @return the auto-increment id of the inserted range, <= 0 if insert is failed
     */
    abstract public long insert (MetadataProto.Range range);

    abstract public boolean update (MetadataProto.Range range);

    abstract public boolean deleteById (long id);
}
