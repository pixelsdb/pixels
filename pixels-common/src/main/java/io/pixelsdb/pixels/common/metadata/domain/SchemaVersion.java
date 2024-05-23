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
package io.pixelsdb.pixels.common.metadata.domain;

import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.List;

/**
 * The schema version of a table.
 * @author hank
 * @create 2023-06-11
 */
public class SchemaVersion extends Base
{
    private List<Column> columns;
    private long transTs;
    private long tableId;

    public SchemaVersion() { }

    public SchemaVersion(MetadataProto.SchemaVersion schemaVersion)
    {
        this.columns = Column.convertColumns(schemaVersion.getColumnsList());
        this.transTs = schemaVersion.getTransTs();
        this.tableId = schemaVersion.getTableId();
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public void setColumns(List<Column> columns)
    {
        this.columns = columns;
    }

    public long getTransTs()
    {
        return transTs;
    }

    public void setTransTs(long transTs)
    {
        this.transTs = transTs;
    }

    public long getTableId()
    {
        return tableId;
    }

    public void setTableId(long tableId)
    {
        this.tableId = tableId;
    }
}
