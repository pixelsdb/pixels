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

import io.pixelsdb.pixels.common.physical.ObjectPath;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.storage.s3.AbstractS3;
import io.pixelsdb.pixels.storage.s3qs.exception.TaskErrorException;
import io.pixelsdb.pixels.storage.s3qs.io.S3QSInputStream;
import io.pixelsdb.pixels.storage.s3qs.io.S3QSOutputStream;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.UnsupportedOperationException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link S3QS} is to write and read the small intermediate files in data shuffling. It is compatible with S3, hence its
 * methods accept the S3 object paths start with s3:// or s3qs://.
 *
 * The main differences from {@link io.pixelsdb.pixels.storage.s3.S3} are that {@link S3QS} only uses synchronous S3 client
 * and creates {@link S3QSInputStream} and {@link S3QSOutputStream} in its open and create methods,
 * and it initializes an SQS client for operating the sqs messages.
 *
 * The {@link #openQueue(String)} method in this class returns a queue backed by an SQS queue that stores the object paths
 * of the intermediate files.
 * @author hank
 * @create 2025-09-17
 */
public final class S3QS extends AbstractS3
{
    private static final String SchemePrefix = Scheme.s3qs.name() + "://";

    private final Map<ShuffleQueueKey, S3Queue> partitionQueues;
    private final int invisibleTime;

    private SqsClient sqs;

    public S3QS()
    {
        this(30);
    }

    public S3QS(int invisibleTime){
        this.connect();
        this.partitionQueues = new ConcurrentHashMap<>();
        this.invisibleTime = invisibleTime;
    }

    private synchronized void connect()
    {
        sqs = SqsClient.builder().build();
        s3 = S3Client.builder().httpClientBuilder(ApacheHttpClient.builder()
                .connectionTimeout(Duration.ofSeconds(ConnTimeoutSec))
                .socketTimeout(Duration.ofSeconds(ConnTimeoutSec))
                .connectionAcquisitionTimeout(Duration.ofSeconds(ConnAcquisitionTimeoutSec))
                .maxConnections(MaxRequestConcurrency)).build();
    }

    @Override
    public void reconnect()
    {
        this.connect();
    }

    @Override
    public Scheme getScheme() { return Scheme.s3qs; }

    @Override
    public String ensureSchemePrefix(String path) throws IOException
    {
        if (path.startsWith(SchemePrefix))
        {
            return path;
        }
        if (path.contains("://"))
        {
            throw new IOException("Path '" + path +
                    "' already has a different scheme prefix than '" + SchemePrefix + "'.");
        }
        return SchemePrefix + path;
    }

    public PhysicalWriter offer(S3QueueMessage mesg) throws IOException
    {
        S3Queue queue = getRegisteredQueue(queueKey(mesg));
        return queue.offer(mesg);
    }

    /**
     * Publish a structured message that does not require writing an S3 object,
     * such as a producer-end marker.
     */
    public void publish(S3QueueMessage mesg) throws IOException
    {
        S3Queue queue = getRegisteredQueue(queueKey(mesg));
        queue.push(mesg);
    }

    public synchronized String registerQueue(String shuffleId, int partitionId,
                                             String queueName, String queueUrl) throws IOException
    {
        ShuffleQueueKey key = queueKey(shuffleId, partitionId);
        S3Queue queue = partitionQueues.get(key);
        if (queue != null)
        {
            if (queueUrl != null && !queueUrl.trim().isEmpty() &&
                    !queue.getQueueUrl().equals(queueUrl))
            {
                throw new IOException("queue " + key + " is already registered with a different URL");
            }
            return queue.getQueueUrl();
        }

        String resolvedQueueUrl = queueUrl;
        if (resolvedQueueUrl == null || resolvedQueueUrl.trim().isEmpty())
        {
            if (queueName == null || queueName.trim().isEmpty())
            {
                throw new IOException("queue name is empty for " + key);
            }
            try
            {
                resolvedQueueUrl = createQueue(queueName);
            }
            catch (RuntimeException e)
            {
                throw new IOException("failed to create queue " + queueName +
                        " for " + key, e);
            }
        }

        queue = openQueue(resolvedQueueUrl);
        partitionQueues.put(key, queue);
        return resolvedQueueUrl;
    }

    /**
     * Remove one local queue wrapper without deleting the remote SQS queue.
     * Remote resources are owned and deleted by the query lifecycle.
     */
    public void unregisterQueue(String shuffleId, int partitionId) throws IOException
    {
        S3Queue queue = partitionQueues.remove(queueKey(shuffleId, partitionId));
        if (queue != null)
        {
            queue.close();
        }
    }

    public String createQueue(String queueName) throws IOException
    {
        try
        {
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributes(Collections.singletonMap(
                            QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(invisibleTime)
                    ))
                    .build();

            sqs.createQueue(createQueueRequest);

            GetQueueUrlResponse getQueueUrlResponse = sqs
                    .getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            return getQueueUrlResponse.queueUrl();
        }
        catch (SqsException e)
        {
            throw new IOException("fail to create sqs queue: " + queueName, e);
        }
    }

    public String getQueueUrl(String queueName) throws IOException
    {
        try
        {
            return sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();
        }
        catch (SqsException e)
        {
            throw new IOException("fail to get sqs queue url: " + queueName, e);
        }
    }

    public void deleteQueue(String queueUrl) throws IOException
    {
        try
        {
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        }
        catch (SqsException e)
        {
            throw new IOException("fail to delete sqs queue: " + queueUrl, e);
        }
    }

    public S3Queue openQueue(String queueUrl)
    {
        return new S3Queue(this, queueUrl, invisibleTime);
    }


    public Map.Entry<String,PhysicalReader> poll(S3QueueMessage mesg, int timeoutSec) throws IOException
    {
        ShuffleQueueKey key = queueKey(mesg);
        S3Queue queue = getRegisteredQueue(key);
        if(queue.isClosed())
        {
            throw new IOException("queue " + key + " is closed.");
        }

        try
        {
            return queue.poll(timeoutSec);
        }
        catch (TaskErrorException e)
        {
            throw new IOException("failed to poll queue " + key, e);
        }
    }

    /**
     * Poll a structured shuffle message from the partition queue.
     *
     * This is the preferred API for S3QS shuffle consumers. It lets callers
     * distinguish DATA messages from producer-end markers before deciding
     * whether to open an S3 object.
     */
    public S3QueuePollResult pollMessage(String shuffleId, int partitionId, int timeoutSec) throws IOException
    {
        ShuffleQueueKey key = queueKey(shuffleId, partitionId);
        S3Queue queue = getRegisteredQueue(key);
        if(queue.isClosed())
        {
            throw new IOException("queue " + key + " is closed.");
        }

        try
        {
            return queue.pollMessage(timeoutSec);
        }
        catch (TaskErrorException e)
        {
            throw new IOException("failed to poll queue " + key, e);
        }
    }

    public int finishWork(S3QueueMessage mesg) throws IOException
    {
        String receiptHandle = mesg.getReceiptHandle();
        S3Queue queue = getRegisteredQueue(queueKey(mesg));
        try
        {
            queue.deleteMessage(receiptHandle);
        }
        catch (SqsException e)
        {
            //TODO: log
            return 2;
        }
        return 0;
    }

    private S3Queue getRegisteredQueue(ShuffleQueueKey key) throws IOException
    {
        S3Queue queue = partitionQueues.get(key);
        if (queue == null)
        {
            throw new IOException("queue is not registered for " + key);
        }
        return queue;
    }

    private static ShuffleQueueKey queueKey(S3QueueMessage message) throws IOException
    {
        if (message == null)
        {
            throw new IOException("queue message is null");
        }
        return queueKey(message.getShuffleId(), message.getPartitionId());
    }

    private static ShuffleQueueKey queueKey(String shuffleId, int partitionId) throws IOException
    {
        try
        {
            return new ShuffleQueueKey(shuffleId, partitionId);
        }
        catch (RuntimeException e)
        {
            throw new IOException("invalid shuffle queue identity", e);
        }
    }
    @Override
    public DataInputStream open(String path) throws IOException
    {
        ObjectPath p = new ObjectPath(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }

        S3QSInputStream inputStream;
        try
        {
            inputStream = new S3QSInputStream(this.s3, p.bucket, p.key);
        }
        catch (Exception e)
        {
            throw new IOException("Failed to open sqsInputStream.", e);
        }
        return new DataInputStream(inputStream);
    }

    /**
     * @return -1 as we do not have a file id for intermediate files
     */
    @Override
    public long getFileId(String path)
    {
        // should not throw exception as this method is called in the constructor of PhysicalS3QSReader.supper.
        return -1;
    }

    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize) throws IOException
    {
        ObjectPath p = new ObjectPath(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        return new DataOutputStream(new S3QSOutputStream(this.s3, p.bucket, p.key, bufferSize));
    }

    @Override
    public boolean supportDirectCopy() { return false; }

    @Override
    public boolean directCopy(String src, String dest)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException
    {
        for (S3Queue queue : partitionQueues.values())
        {
            queue.close();
        }
        this.partitionQueues.clear();
        if (this.sqs != null)
        {
            this.sqs.close();
        }
        if (s3 != null)
        {
            s3.close();
        }
    }

    public void refresh() throws IOException
    {
        for (S3Queue queue : partitionQueues.values())
        {
            queue.close();
        }
        this.partitionQueues.clear();
    }

    public SqsClient getSqsClient()
    {
        return sqs;
    }
}
