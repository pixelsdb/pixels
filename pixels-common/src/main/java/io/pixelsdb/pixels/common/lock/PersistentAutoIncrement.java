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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The auto increment id that is backed by etcd auto increment.
 * It acquires a segment of auto increment ids from etcd auto increment each time and allocates
 * auto increment ids to clients from the segment using atomics. Thus, it is fast and persistent.
 *
 * @author hank
 * @create 2024-09-27
 */
public class PersistentAutoIncrement
{
    private final String idKey;
    private final Lock lock = new ReentrantLock();
    private volatile long id;
    private volatile long count;

    public PersistentAutoIncrement(String idKey) throws EtcdException
    {
        this.idKey = idKey;
        EtcdAutoIncrement.InitId(idKey);
        EtcdAutoIncrement.Segment segment = EtcdAutoIncrement.GenerateId(idKey, Constants.AI_DEFAULT_STEP);
        this.id = segment.getStart();
        this.count = segment.getLength();
    }

    public long getAndIncrement() throws EtcdException
    {
        this.lock.lock();
        if (this.count > 0)
        {
            long value = this.id++;
            this.count--;
            this.lock.unlock();
            return value;
        }
        else
        {
            EtcdAutoIncrement.GenerateId(idKey, Constants.AI_DEFAULT_STEP, segment -> {
                this.id = segment.getStart();
                this.count = segment.getLength();
            });
            // no need to release the reentrant lock
            return this.getAndIncrement();
        }
    }
}
