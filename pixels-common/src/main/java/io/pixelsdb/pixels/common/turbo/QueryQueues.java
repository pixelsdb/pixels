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
package io.pixelsdb.pixels.common.turbo;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * This is only used by pixels-turbo (i.e., the AUTO mode of cloud.function.switch in pixels-trino)
 * to queue the running queries and choose the query executor.
 * @author hank
 * @create 2022-10-24
 */
public class QueryQueues
{
    private final ArrayBlockingQueue<Long> mppQueue;
    private final ArrayBlockingQueue<Long> cfQueue;

    private QueryQueues(int clusterQueueCapacity, int lambdaQueueCapacity)
    {
        this.mppQueue = new ArrayBlockingQueue<>(clusterQueueCapacity);
        this.cfQueue = new ArrayBlockingQueue<>(lambdaQueueCapacity);
    }

    private static QueryQueues instance = null;

    public synchronized static QueryQueues Instance()
    {
        if (instance == null)
        {
            int clusterQueueCapacity = Integer.parseInt(
                    ConfigFactory.Instance().getProperty("scaling.mpp.queue.capacity"));
            int lambdaQueueCapacity = Integer.parseInt(
                    ConfigFactory.Instance().getProperty("scaling.cf.queue.capacity"));
            instance = new QueryQueues(clusterQueueCapacity, lambdaQueueCapacity);
        }
        return instance;
    }

    public synchronized ExecutorType Enqueue(long transId)
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
}
