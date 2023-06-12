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
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.daemon.MetadataProto;

public class Table extends Base
{
    private String name;
    private String type;
    private Storage.Scheme storageScheme;
    private long rowCount;
    private long schemaId;

    public Table()
    {
    }

    public Table(MetadataProto.Table table)
    {
        this.name = table.getName();
        this.type = table.getType();
        this.storageScheme = Storage.Scheme.from(table.getStorageScheme());
        this.rowCount = table.getRowCount();
        this.schemaId = table.getSchemaId();
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

    public Storage.Scheme getStorageScheme()
    {
        return storageScheme;
    }

    public void setStorageScheme(Storage.Scheme storageScheme)
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

    @Override
    public String toString()
    {
        return "Table{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", storageScheme='" + storageScheme + '\'' +
                ", rowCount=" + rowCount +
                ", schemaId=" + schemaId + '}';
    }
}
