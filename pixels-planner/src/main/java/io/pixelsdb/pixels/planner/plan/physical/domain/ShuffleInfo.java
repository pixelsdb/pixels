/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.planner.plan.physical.domain;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * The shared metadata of one shuffle edge between producer and consumer stages.
 *
 * @author Haoting Yan
 * @create 2026-06-17
 */
public class ShuffleInfo
{
    /**
     * A query-unique id for this shuffle edge.
     */
    private String shuffleId;
    /**
     * The storage used by this shuffle. For S3QS this should be Storage.Scheme.s3qs.
     */
    private StorageInfo storageInfo;
    /**
     * The object prefix for shuffle data objects.
     */
    private String objectPathPrefix;
    /**
     * The number of hash partitions produced by this shuffle.
     */
    private int numPartitions;
    /**
     * The number of immutable logical producer tasks expected for this shuffle.
     * Physical workers may scale independently and execute these tasks.
     */
    private int logicalProducerTaskCount;
    /**
     * The number of consumer workers expected for this shuffle.
     */
    private int consumerCount;
    /**
     * The max long-poll time in seconds for queue consumers.
     */
    private int pollTimeoutSeconds;
    /**
     * The queues assigned to hash partitions.
     */
    private List<ShuffleQueueInfo> queues;

    /**
     * Default constructor for Jackson.
     */
    public ShuffleInfo() { }

    public ShuffleInfo(String shuffleId, StorageInfo storageInfo, String objectPathPrefix,
                       int numPartitions, int logicalProducerTaskCount, int consumerCount,
                       int pollTimeoutSeconds, List<ShuffleQueueInfo> queues)
    {
        this.shuffleId = shuffleId;
        this.storageInfo = storageInfo;
        this.objectPathPrefix = objectPathPrefix;
        this.numPartitions = numPartitions;
        this.logicalProducerTaskCount = logicalProducerTaskCount;
        this.consumerCount = consumerCount;
        this.pollTimeoutSeconds = pollTimeoutSeconds;
        this.queues = queues == null ? null : ImmutableList.copyOf(queues);
    }

    public String getShuffleId()
    {
        return shuffleId;
    }

    public void setShuffleId(String shuffleId)
    {
        this.shuffleId = shuffleId;
    }

    public StorageInfo getStorageInfo()
    {
        return storageInfo;
    }

    public void setStorageInfo(StorageInfo storageInfo)
    {
        this.storageInfo = storageInfo;
    }

    public String getObjectPathPrefix()
    {
        return objectPathPrefix;
    }

    public void setObjectPathPrefix(String objectPathPrefix)
    {
        this.objectPathPrefix = objectPathPrefix;
    }

    public int getNumPartitions()
    {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions)
    {
        this.numPartitions = numPartitions;
    }

    public int getLogicalProducerTaskCount()
    {
        return logicalProducerTaskCount;
    }

    public void setLogicalProducerTaskCount(int logicalProducerTaskCount)
    {
        this.logicalProducerTaskCount = logicalProducerTaskCount;
    }

    public int getConsumerCount()
    {
        return consumerCount;
    }

    public void setConsumerCount(int consumerCount)
    {
        this.consumerCount = consumerCount;
    }

    public int getPollTimeoutSeconds()
    {
        return pollTimeoutSeconds;
    }

    public void setPollTimeoutSeconds(int pollTimeoutSeconds)
    {
        this.pollTimeoutSeconds = pollTimeoutSeconds;
    }

    public List<ShuffleQueueInfo> getQueues()
    {
        return queues;
    }

    public void setQueues(List<ShuffleQueueInfo> queues)
    {
        this.queues = queues;
    }
}
