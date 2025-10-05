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
    private final long defaultStep;
    private final boolean interProc;
    private volatile long id;
    private volatile long count;

    /**
     * @param idKey the key of this auto increment id in etcd
     * @param interProc true if the backed key-value of this increment id in etcd
     *                  may be simultaneously accessed by multiple processes
     * @throws EtcdException when fail to interact with the backed etcd instance
     */
    public PersistentAutoIncrement(String idKey, boolean interProc) throws EtcdException
    {
        this.idKey = idKey;
        this.defaultStep = Constants.AI_DEFAULT_STEP;
        this.interProc = interProc;
        EtcdAutoIncrement.InitId(idKey, interProc);
        EtcdAutoIncrement.Segment segment = EtcdAutoIncrement.GenerateId(idKey, this.defaultStep, interProc);
        this.id = segment.getStart();
        this.count = segment.getLength();
    }

    /**
     * @param idKey the key of this auto increment id in etcd
     * @param defaultStep the default step for allocating row ids from etcd, determining the frequency of updating the
     *                    backed key-value of this auto increment id in etcd
     * @param interProc true if the backed key-value of this increment id in etcd
     *                  may be simultaneously accessed by multiple processes
     * @throws EtcdException when fail to interact with the backed etcd instance
     */
    public PersistentAutoIncrement(String idKey, long defaultStep, boolean interProc) throws EtcdException
    {
        this.idKey = idKey;
        this.defaultStep = defaultStep;
        this.interProc = interProc;
        EtcdAutoIncrement.InitId(idKey, interProc);
        EtcdAutoIncrement.Segment segment = EtcdAutoIncrement.GenerateId(idKey, this.defaultStep, interProc);
        this.id = segment.getStart();
        this.count = segment.getLength();
    }

    /**
     * Get the current value of this auto increment and increase it by one.
     * @return the current value of this auto increment.
     * @throws EtcdException when fail to interact with the backed etcd instance.
     */
    public long getAndIncrement() throws EtcdException
    {
        this.lock.lock();
        try
        {
            if (this.count > 0)
            {
                long value = this.id++;
                this.count--;
                return value;
            }
            else
            {
                EtcdAutoIncrement.GenerateId(this.idKey, this.defaultStep, this.interProc, segment -> {
                    this.id = segment.getStart();
                    this.count = segment.getLength();
                });
                // no need to release the reentrant lock
                return this.getAndIncrement();
            }
        }
        finally
        {
            this.lock.unlock();
        }
    }

    /**
     * Get the current value of this auto increment and increase it by the given batch size.
     * <br/>
     * <b>Note: It is recommended that the etcdStep in the constructor of this class is significantly larger than
     * the batchSize here. Thus, the etcd overhead would not be significant.</b>
     * @param batchSize the given batch size
     * @return the current value of this auto increment.
     * @throws EtcdException when fail to interact with the backed etcd instance.
     */
    public long getAndIncrement(int batchSize) throws EtcdException
    {
        this.lock.lock();
        try
        {
            if (this.count >= batchSize)
            {
                long value = this.id;
                this.id += batchSize;
                this.count -= batchSize;
                return value;
            }
            else
            {
                // Issue #986: avoid infinite recursion by ensuring step >= batchSize.
                // Issue #1099: ensure better performance by setting the step to >= batchSize * 10L.
                long step = Math.max(this.defaultStep, batchSize * 10L);
                EtcdAutoIncrement.GenerateId(this.idKey, step, this.interProc, segment -> {
                    this.id = segment.getStart();
                    this.count = segment.getLength();
                });
                return this.getAndIncrement(batchSize);
            }
        }
        finally
        {
            this.lock.unlock();
        }
    }
}
