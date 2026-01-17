/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.storage.s3qs;

/**
 * Unified message format for communication between worker and s3qs.
 * @author yanhaoting
 * @create 2025-12-19
 */
public class S3QueueMessage
{
    private String objectPath;
    private int workerNum = 0;
    private int partitionNum = 0;
    private boolean endWork = false;//specific to workers, indicating whether his work is finished
    private String receiptHandle = "";//specific to consumers, indicating which mesg is consumed.
    private long timestamp = System.currentTimeMillis();
    private String metadata = "";

    public S3QueueMessage()
    {
    }

    public S3QueueMessage(String objectPath)
    {
        this.objectPath = objectPath;
        this.timestamp = System.currentTimeMillis();
    }

    public String getObjectPath() 
    {
        return objectPath;
    }

    public S3QueueMessage setObjectPath(String objectPath)
    {
        this.objectPath = objectPath;
        return this;
    }

    public int getWorkerNum() {
        return this.workerNum;
    }

    public S3QueueMessage setWorkerNum(int WorkerNum)
    {
        this.workerNum = WorkerNum;
        return this;
    }

    public int getPartitionNum() 
    {
        return this.partitionNum;
    }

    public S3QueueMessage setPartitionNum(int PartitionNum)
    {
        this.partitionNum = PartitionNum;
        return this;
    }

    public boolean getEndWork() 
    {
        return this.endWork;
    }

    public S3QueueMessage setEndwork(boolean endwork)
    {
        this.endWork = endwork;
        return this;
    }

    public String getReceiptHandle() 
    {
        return this.receiptHandle;
    }

    public S3QueueMessage setReceiptHandle(String ReceiptHandle)
    {
        this.receiptHandle = ReceiptHandle;
        return this;
    }

    public long getTimestamp() 
    {
        return timestamp;
    }

    public S3QueueMessage setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
        return this;
    }

    public String getMetadata() 
    {
        return metadata;
    }

    public S3QueueMessage setMetadata(String metadata)
    {
        this.metadata = metadata;
        return this;
    }
}
