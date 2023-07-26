/*
 * Copyright 2021-2023 PixelsDB.
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
package io.pixelsdb.pixels.common.queue;

import com.alibaba.fastjson.JSON;

import java.util.Objects;

/**
 * @author hank
 * @create 2023-07-26
 */
public class Task<T>
{
    private final String id;
    private final T payload;

    public Task(String id, T payload)
    {
        this.id = id;
        this.payload = payload;
    }

    public Task(String id, String payloadJson, Class<T> clazz)
    {
        this.id = id;
        this.payload = JSON.parseObject(payloadJson, clazz);
    }

    public String getPayloadJson()
    {
        return JSON.toJSONString(this.payload);
    }

    public String getId()
    {
        return id;
    }

    public T getPayload()
    {
        return payload;
    }

    @Override
    public int hashCode()
    {
        return 31 * Objects.hashCode(this.id) + Objects.hashCode(this.payload);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof Task))
        {
            return false;
        }
        Task<?> that = (Task<?>) obj;
        return Objects.equals(this.id, that.id) && Objects.equals(this.payload, that.payload);
    }

    @Override
    public String toString()
    {
        return JSON.toJSONString(this);
    }
}
