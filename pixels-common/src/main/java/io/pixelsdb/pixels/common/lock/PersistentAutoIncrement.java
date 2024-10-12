/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.common.lock;

import io.pixelsdb.pixels.common.exception.EtcdException;
import io.pixelsdb.pixels.common.utils.Constants;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The auto increment id that is backed by etcd auto increment.
 * It acquires a segment of auto increment ids from etcd auto increment each time and allocates
 * auto increment ids to clients from the segment using atomics. Thus, it is fast and persistent.
 * @author hank
 * @create 2024-09-27
 */
public class PersistentAutoIncrement
{
    private final String idKey;
    private final AtomicLong id;
    private final AtomicLong count;

    public PersistentAutoIncrement(String idKey) throws EtcdException
    {
        this.idKey = idKey;
        EtcdAutoIncrement.InitId(idKey);
        EtcdAutoIncrement.Segment segment = EtcdAutoIncrement.GenerateId(idKey, Constants.AI_DEFAULT_STEP);
        this.id = new AtomicLong(segment.getStart());
        this.count = new AtomicLong(segment.getLength());
    }

    public long getAndIncrement() throws EtcdException
    {
        long value = this.id.getAndIncrement();
        if (this.count.getAndDecrement() > 0 && value < this.id.get())
        {
            return value;
        }
        else
        {
            EtcdAutoIncrement.GenerateId(idKey, Constants.AI_DEFAULT_STEP, segment -> {
                this.id.set(segment.getStart());
                this.count.set(segment.getLength());
            });
            return this.getAndIncrement();
        }
    }
}
