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
 * @create 2024-06-08
 */
public abstract class FileDao implements Dao<MetadataProto.File>
{
    @Override
    public abstract MetadataProto.File getById(long id);

    @Override
    public List<MetadataProto.File> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    public abstract List<MetadataProto.File> getAllByPathId(long pathId);

    public boolean save (MetadataProto.File file)
    {
        if (exists(file))
        {
            return update(file);
        }
        else
        {
            return insert(file) > 0;
        }
    }

    abstract public boolean exists (MetadataProto.File file);

    /**
     * Insert the file into metadata.
     * @param file the file
     * @return the auto-increment id of the inserted file, <= 0 if insert is failed
     */
    abstract public long insert (MetadataProto.File file);

    abstract public boolean update (MetadataProto.File file);

    abstract public boolean deleteByIds (List<Long> ids);
}
