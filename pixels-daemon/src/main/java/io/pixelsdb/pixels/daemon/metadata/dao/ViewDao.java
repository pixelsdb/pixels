/*
 * Copyright 2022 PixelsDB.
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
 */
public abstract class ViewDao implements Dao<MetadataProto.View>
{
    public ViewDao() {}

    @Override
    abstract public MetadataProto.View getById(long id);

    @Override
    public List<MetadataProto.View> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    abstract public MetadataProto.View getByNameAndSchema (String name, MetadataProto.Schema schema);

    abstract public List<MetadataProto.View> getByName(String name);

    abstract public List<MetadataProto.View> getBySchema(MetadataProto.Schema schema);

    public boolean save (MetadataProto.View view)
    {
        if (exists(view))
        {
            return update(view);
        }
        else
        {
            return insert(view);
        }
    }

    /**
     * Return true if the view with the same id or with the same db_id and view name exists.
     * @param view
     * @return
     */
    abstract public boolean exists (MetadataProto.View view);

    abstract public boolean insert (MetadataProto.View view);

    abstract public boolean update (MetadataProto.View view);

    /**
     * Delete a view.
     * @param name
     * @param schema
     * @return
     */
    abstract public boolean deleteByNameAndSchema (String name, MetadataProto.Schema schema);
}
