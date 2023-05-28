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
 * @author hank
 * @create 2022-10-24
 */
public class QueryQueues
{
    public enum ExecutorType
    {
        Cluster, Lambda, None // None means the query should wait for execution.
    }

    private final ArrayBlockingQueue<Long> clusterQueue;
    private final ArrayBlockingQueue<Long> lambdaQueue;

    private QueryQueues(int clusterQueueCapacity, int lambdaQueueCapacity)
    {
        this.clusterQueue = new ArrayBlockingQueue<>(clusterQueueCapacity);
        this.lambdaQueue = new ArrayBlockingQueue<>(lambdaQueueCapacity);
    }

    private static QueryQueues instance = null;

    public synchronized static QueryQueues Instance()
    {
        if (instance == null)
        {
            int clusterQueueCapacity = Integer.parseInt(
                    ConfigFactory.Instance().getProperty("scaling.cluster.queue.capacity"));
            int lambdaQueueCapacity = Integer.parseInt(
                    ConfigFactory.Instance().getProperty("scaling.serverless.queue.capacity"));
            instance = new QueryQueues(clusterQueueCapacity, lambdaQueueCapacity);
        }
        return instance;
    }

    public synchronized ExecutorType Enqueue(long transId)
    {
        if (this.clusterQueue.offer(transId))
        {
            return ExecutorType.Cluster;
        }
        if (this.lambdaQueue.offer(transId))
        {
            return ExecutorType.Lambda;
        }
        return ExecutorType.None;
    }

    public boolean Dequeue(long transId, ExecutorType executorType)
    {
        if (executorType == ExecutorType.Cluster)
        {
            return this.clusterQueue.remove(transId);
        }
        if (executorType == ExecutorType.Lambda)
        {
            return this.lambdaQueue.remove(transId);
        }
        return false;
    }
}
