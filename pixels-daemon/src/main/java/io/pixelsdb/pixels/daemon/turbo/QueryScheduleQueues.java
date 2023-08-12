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
package io.pixelsdb.pixels.daemon.turbo;

import io.pixelsdb.pixels.common.turbo.ExecutorType;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * This is only used by pixels-turbo to queue the running queries and choose the query executor.
 *
 * @author hank
 * @create 2022-10-24
 */
public class QueryScheduleQueues
{
    private final ArrayBlockingQueue<Long> mppQueue;
    private final ArrayBlockingQueue<Long> cfQueue;

    private QueryScheduleQueues(int mppQueueCapacity, int cfQueueCapacity)
    {
        this.mppQueue = new ArrayBlockingQueue<>(mppQueueCapacity);
        this.cfQueue = new ArrayBlockingQueue<>(cfQueueCapacity);
    }

    private static final QueryScheduleQueues instance;

    static
    {
        int mppQueueCapacity = Integer.parseInt(
                ConfigFactory.Instance().getProperty("scaling.mpp.queue.capacity"));
        int cfQueueCapacity = Integer.parseInt(
                ConfigFactory.Instance().getProperty("scaling.cf.queue.capacity"));
        instance = new QueryScheduleQueues(mppQueueCapacity, cfQueueCapacity);
    }

    protected static QueryScheduleQueues Instance()
    {
        return instance;
    }

    public boolean EnqueueMpp(long transId)
    {
        return this.mppQueue.offer(transId);
    }

    public ExecutorType Enqueue(long transId)
    {
        if (this.mppQueue.offer(transId))
        {
            return ExecutorType.MPP;
        }
        if (this.cfQueue.offer(transId))
        {
            return ExecutorType.CF;
        }
        return ExecutorType.PENDING;
    }

    public boolean Dequeue(long transId, ExecutorType executorType)
    {
        if (executorType == ExecutorType.MPP)
        {
            return this.mppQueue.remove(transId);
        }
        if (executorType == ExecutorType.CF)
        {
            return this.cfQueue.remove(transId);
        }
        return false;
    }

    public int getMppConcurrency()
    {
        return this.mppQueue.size();
    }

    public int getCfConcurrency()
    {
        return this.cfQueue.size();
    }

    public int getMppSlots()
    {
        return this.mppQueue.remainingCapacity();
    }

    public int getCfSlots()
    {
        return this.cfQueue.remainingCapacity();
    }
}
