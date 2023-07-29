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
package io.pixelsdb.pixels.common.task;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2023-07-29
 */
public class Leaseholder
{
    private final String ownerId;
    private final Map<String, String> ownerMetadata;

    public Leaseholder(String ownerId)
    {
        this.ownerId = requireNonNull(ownerId, "owner id is null");
        this.ownerMetadata = new HashMap<>();
    }

    public String getOwnerId()
    {
        return ownerId;
    }

    public void addOwnerMetadata(String key, String value)
    {
        this.ownerMetadata.put(key, value);
    }

    public void getOwnerMetadata(String key)
    {
        this.ownerMetadata.get(key);
    }

    @Override
    public int hashCode()
    {
        return this.ownerId.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof Leaseholder))
        {
            return false;
        }
        Leaseholder that = (Leaseholder) obj;
        return Objects.equals(this.ownerId, that.ownerId);
    }

    @Override
    public String toString()
    {
        return JSON.toJSONString(this);
    }
}
