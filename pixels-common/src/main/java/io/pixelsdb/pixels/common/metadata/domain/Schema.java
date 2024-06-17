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

import io.pixelsdb.pixels.daemon.MetadataProto;

public class Schema extends Base
{
    private String name;
    private String desc;

    public Schema()
    {
    }

    public Schema(MetadataProto.Schema schema)
    {
        this.name = schema.getName();
        this.desc = schema.getDesc();
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getDesc()
    {
        return desc;
    }

    public void setDesc(String desc)
    {
        this.desc = desc;
    }

    @Override
    public String toString()
    {
        return "Schema{" +
                "name='" + name + '\'' +
                ", desc='" + desc + '\'' + '}';
    }

    @Override
    public MetadataProto.Schema toProto()
    {
        return MetadataProto.Schema.newBuilder().setId(this.getId()).setName(this.name).setDesc(this.desc).build();
    }
}
