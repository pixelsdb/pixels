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
 * @create 2023-06-11
 */
public abstract class SchemaVersionDao implements Dao<MetadataProto.SchemaVersion>
{
    @Override
    public abstract MetadataProto.SchemaVersion getById(long id);

    @Override
    public List<MetadataProto.SchemaVersion> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    /**
     * Insert the schema version into metadata.
     * @param schemaVersion the schema version
     * @return the auto-increment id of the inserted schema version, <= 0 if insert is failed
     */
    public abstract long insert (MetadataProto.SchemaVersion schemaVersion);
}
