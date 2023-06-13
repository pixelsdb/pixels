/*
 * Copyright 2023 PixelsDB.
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
 * @create 2023-06-09
 */
public abstract class PathDao implements Dao<MetadataProto.Path>
{
    @Override
    public abstract MetadataProto.Path getById(long id);

    @Override
    public List<MetadataProto.Path> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    public abstract List<MetadataProto.Path> getAllByLayoutId(long layoutId);

    public abstract List<MetadataProto.Path> getAllByRangeId(long rangeId);

    public boolean save (MetadataProto.Path path)
    {
        if (exists(path))
        {
            return update(path);
        }
        else
        {
            return insert(path) > 0;
        }
    }

    abstract public boolean exists (MetadataProto.Path path);

    /**
     * Insert the path into metadata.
     * @param path the path
     * @return the auto-increment id of the inserted path, <= 0 if insert is failed
     */
    abstract public long insert (MetadataProto.Path path);

    abstract public boolean update (MetadataProto.Path path);

    abstract public boolean deleteByIds (List<Long> ids);
}
