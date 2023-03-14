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
package io.pixelsdb.pixels.common.metadata.domain;

import com.alibaba.fastjson.annotation.JSONField;
import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.ArrayList;
import java.util.List;

public class Table extends Base
{
    private String name;
    private String type;
    private String storageScheme;
    private long rowCount;
    private long schemaId;
    private List<Long> columnIds = new ArrayList<>();

    public Table()
    {
    }

    public Table(MetadataProto.Table table)
    {
        this.name = table.getName();
        this.type = table.getType();
        this.storageScheme = table.getStorageScheme();
        this.rowCount = table.getRowCount();
        this.schemaId = table.getSchemaId();
        this.columnIds.addAll(table.getColumnIdsList());
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public String getStorageScheme()
    {
        return storageScheme;
    }

    public void setStorageScheme(String storageScheme)
    {
        this.storageScheme = storageScheme;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    @JSONField(serialize = false)
    public void setRowCount(long rowCount)
    {
        this.rowCount = rowCount;
    }

    public long getSchemaId()
    {
        return schemaId;
    }

    public void setSchemaId(long schemaId)
    {
        this.schemaId = schemaId;
    }

    @JSONField(serialize = false)
    public List<Long> getColumnIds()
    {
        return columnIds;
    }

    public void setColumnIds(List<Long> columnIds)
    {
        this.columnIds = columnIds;
    }

    public void addColumnId(long columnId)
    {
        this.columnIds.add(columnId);
    }

    @Override
    public String toString()
    {
        return "Table{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", storageScheme='" + storageScheme + '\'' +
                ", rowCount=" + rowCount +
                ", schemaId=" + schemaId +
                ", columnIds=" + columnIds +
                '}';
    }
}
