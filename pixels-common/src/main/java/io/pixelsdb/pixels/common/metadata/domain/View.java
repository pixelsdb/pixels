/*
 * Copyright 2022 PixelsDB.
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

public class View extends Base
{
    private String name;
    private String type;
    private String data;
    private long schemaId;

    public View()
    {
    }

    public View(MetadataProto.View view)
    {
        this.name = view.getName();
        this.type = view.getType();
        this.data = view.getData();
        this.schemaId = view.getSchemaId();
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

    @JSONField(serialize = false)
    public String getData()
    {
        return data;
    }

    public void setData(String data)
    {
        this.data = data;
    }

    public long getSchemaId()
    {
        return schemaId;
    }

    public void setSchema(long schemaId)
    {
        this.schemaId = schemaId;
    }

    @Override
    public String toString()
    {
        return "View{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", data='" + data + '\'' +
                ", schemaId=" + schemaId +
                '}';
    }

    @Override
    public MetadataProto.View toProto()
    {
        return MetadataProto.View.newBuilder().setId(this.getId()).setName(this.name).setType(this.type)
                .setData(this.data).setSchemaId(this.schemaId).build();
    }
}
