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

import com.google.protobuf.MessageOrBuilder;

import java.io.Serializable;

/**
 * @author hank
 */
public abstract class Base implements Serializable
{
    private long id;

    public long getId()
    {
        return id;
    }

    public void setId(long id)
    {
        this.id = id;
    }

    public abstract MessageOrBuilder toProto();

    @Override
    public int hashCode()
    {
        return (int) this.id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof Base)
        {
            return this.id == ((Base) o).id;
        }
        return false;
    }
}
