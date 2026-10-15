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

/**
 * The queue endpoint for one hash partition in a shuffle.
 *
 * @author Haoting Yan
 * @create 2026-06-17
 */
public class ShuffleQueueInfo
{
    /**
     * The hash partition id consumed through this queue.
     */
    private int partitionId;
    /**
     * The stable queue name for this shuffle partition.
     */
    private String queueName;
    /**
     * The queue url used by shuffle producers and consumers after the queue is created or resolved.
     */
    private String queueUrl;

    /**
     * Default constructor for Jackson.
     */
    public ShuffleQueueInfo() { }

    public ShuffleQueueInfo(int partitionId, String queueName, String queueUrl)
    {
        this.partitionId = partitionId;
        this.queueName = queueName;
        this.queueUrl = queueUrl;
    }

    public int getPartitionId()
    {
        return partitionId;
    }

    public void setPartitionId(int partitionId)
    {
        this.partitionId = partitionId;
    }

    public String getQueueName()
    {
        return queueName;
    }

    public void setQueueName(String queueName)
    {
        this.queueName = queueName;
    }

    public String getQueueUrl()
    {
        return queueUrl;
    }

    public void setQueueUrl(String queueUrl)
    {
        this.queueUrl = queueUrl;
    }
}
