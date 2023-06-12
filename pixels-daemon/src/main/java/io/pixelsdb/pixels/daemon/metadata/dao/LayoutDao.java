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
package io.pixelsdb.pixels.daemon.metadata.dao;

import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.List;

/**
 * @author hank
 */
public abstract class LayoutDao implements Dao<MetadataProto.Layout>
{
    public LayoutDao() {}

    @Override
    abstract public MetadataProto.Layout getById(long id);

    @Override
    public List<MetadataProto.Layout> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    abstract public MetadataProto.Layout getLatestByTable(MetadataProto.Table table,
                                                 MetadataProto.GetLayoutRequest.PermissionRange permissionRange);

    public List<MetadataProto.Layout> getAllByTable (MetadataProto.Table table)
    {
        return getByTable(table, -1, MetadataProto.GetLayoutRequest.PermissionRange.ALL);
    }

    /**
     * get layout of a table by version and permission range.
     * @param table
     * @param version < 0 to get all versions of layouts.
     * @return
     */
    abstract public List<MetadataProto.Layout> getByTable (MetadataProto.Table table, long version,
                                                          MetadataProto.GetLayoutRequest.PermissionRange permissionRange);

    public boolean save (MetadataProto.Layout layout)
    {
        if (exists(layout))
        {
            return update(layout);
        }
        else
        {
            return insert(layout) > 0;
        }
    }

    abstract public boolean exists (MetadataProto.Layout layout);

    /**
     * Insert the layout into metadata.
     * @param layout the layout
     * @return the auto-increment id of the inserted layout, <= 0 if insert is failed
     */
    abstract public long insert (MetadataProto.Layout layout);

    abstract public boolean update (MetadataProto.Layout layout);
}
