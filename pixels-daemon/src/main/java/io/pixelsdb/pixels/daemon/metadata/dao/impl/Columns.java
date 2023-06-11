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
package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.ArrayList;
import java.util.List;

/**
 * The set of column ids belong to a schema version or peer path in the metadata.
 * @author hank
 * @create 2023-06-09
 */
public class Columns
{
    private List<Long> columnIds;

    public Columns() { }

    public Columns(List<MetadataProto.Column> columns)
    {
        this.columnIds = new ArrayList<>(columns.size());
        for (MetadataProto.Column column : columns)
        {
            this.columnIds.add(column.getId());
        }
    }

    public List<Long> getColumnIds()
    {
        return columnIds;
    }

    public void setColumnIds(List<Long> columnIds)
    {
        this.columnIds = columnIds;
    }
}
