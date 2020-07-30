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
public abstract class SchemaDao implements Dao<MetadataProto.Schema>
{
    public SchemaDao() {}

    @Override
    abstract public MetadataProto.Schema getById(long id);

    @Override
    abstract public List<MetadataProto.Schema> getAll();

    abstract public MetadataProto.Schema getByName(String name);

    public boolean save (MetadataProto.Schema schema)
    {
        if (exists(schema))
        {
            return update(schema);
        }
        else
        {
            return insert(schema);
        }
    }

    abstract public boolean exists (MetadataProto.Schema schema);

    abstract public boolean insert (MetadataProto.Schema schema);

    abstract public boolean update (MetadataProto.Schema schema);

    /**
     * We ensure cascade delete and update in the metadata database.
     * If you delete a schema by this method, all the tables, layouts and columns of the schema
     * will be deleted.
     * @param name
     * @return
     */
    abstract public boolean deleteByName (String name);
}
