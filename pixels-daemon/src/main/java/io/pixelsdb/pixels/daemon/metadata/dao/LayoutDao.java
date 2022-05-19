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

    protected MetadataProto.Layout.Permission convertPermission (short permission)
    {
        switch (permission)
        {
            case -1:
                return MetadataProto.Layout.Permission.DISABLED;
            case 0:
                return MetadataProto.Layout.Permission.READ_ONLY;
            case 1:
                return MetadataProto.Layout.Permission.READ_WRITE;
        }
        return MetadataProto.Layout.Permission.DISABLED;
    }

    @Override
    public List<MetadataProto.Layout> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    abstract public MetadataProto.Layout getLatestByRegion(MetadataProto.Region region,
                                                 MetadataProto.GetLayoutRequest.PermissionRange permissionRange);

    public List<MetadataProto.Layout> getAllByRegion (MetadataProto.Region region)
    {
        return getByRegion(region, -1, MetadataProto.GetLayoutRequest.PermissionRange.ALL);
    }

    /**
     * get layout of a table by version and permission range.
     * @param region
     * @param version < 0 to get all versions of layouts.
     * @return
     */
    abstract public List<MetadataProto.Layout> getByRegion (MetadataProto.Region region, int version,
                                                          MetadataProto.GetLayoutRequest.PermissionRange permissionRange);

    public boolean save (MetadataProto.Layout layout)
    {
        if (exists(layout))
        {
            return update(layout);
        }
        else
        {
            return insert(layout);
        }
    }

    abstract public boolean exists (MetadataProto.Layout layout);

    abstract public boolean insert (MetadataProto.Layout layout);

    protected short convertPermission (MetadataProto.Layout.Permission permission)
    {
        switch (permission)
        {
            case DISABLED:
                return -1;
            case READ_ONLY:
                return 0;
            case READ_WRITE:
                return -1;
        }
        return -1;
    }

    abstract public boolean update (MetadataProto.Layout layout);
}
