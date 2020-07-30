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
public abstract class TableDao implements Dao<MetadataProto.Table>
{
    public TableDao() {}

    @Override
    abstract public MetadataProto.Table getById(long id);

    @Override
    public List<MetadataProto.Table> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    abstract public MetadataProto.Table getByNameAndSchema (String name, MetadataProto.Schema schema);

    abstract public List<MetadataProto.Table> getByName(String name);

    abstract public List<MetadataProto.Table> getBySchema(MetadataProto.Schema schema);

    public boolean save (MetadataProto.Table table)
    {
        if (exists(table))
        {
            return update(table);
        }
        else
        {
            return insert(table);
        }
    }

    /**
     * If the table with the same id or with the same db_id and table name exists,
     * this method returns false.
     * @param table
     * @return
     */
    abstract public boolean exists (MetadataProto.Table table);

    abstract public boolean insert (MetadataProto.Table table);

    abstract public boolean update (MetadataProto.Table table);

    /**
     * We ensure cascade delete and update in the metadata database.
     * If you delete a table by this method, all the layouts and columns of the table
     * will be deleted.
     * @param name
     * @param schema
     * @return
     */
    abstract public boolean deleteByNameAndSchema (String name, MetadataProto.Schema schema);
}
