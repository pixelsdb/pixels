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

import io.pixelsdb.pixels.common.metadata.domain.Ordered;
import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.List;

/**
 * @author hank
 */
public abstract class ColumnDao implements Dao<MetadataProto.Column>
{
    public ColumnDao() {}

    @Override
    abstract public MetadataProto.Column getById(long id);

    @Override
    public List<MetadataProto.Column> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    abstract public List<MetadataProto.Column> getByTable(MetadataProto.Table table, boolean getStatistics);

    abstract public Ordered getOrderedByTable(MetadataProto.Table table);

    abstract public boolean update(MetadataProto.Column column);

    /**
     * Insert a batch of columns into the metadata. This method is used in table creation.
     * Statistics are ignored and not inserted in to the metadata database.
     *
     * @param table the table that the columns belong to
     * @param columns the columns to insert, statistics are ignored
     * @return the number of columns that are inserted
     */
    abstract public int insertBatch (MetadataProto.Table table, List<MetadataProto.Column> columns);

    abstract public boolean deleteByTable (MetadataProto.Table table);
}
