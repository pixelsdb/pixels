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

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.PhysicalWriterUtil;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.storage.s3qs.exception.TaskErrorException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT;

/**
 * This is the queue to read from and write to s3+sqs.
 * It is thread safe. Using multiple threads to poll and offer the queue can improve the data transfer throughput
 * if not blocked by the network bandwidth.
 *
 * @author hank
 * @create 2025-09-26
 */
public class S3Queue implements Closeable
{
    private static final int MAX_POLL_BATCH_SIZE = 10;
    private static final int MAX_POLL_WAIT_SECS = 20;
    private static final int POLL_BATCH_SIZE;

    static
    {
        int batchSize = Integer.parseInt(ConfigFactory.Instance().getProperty("s3qs.poll.batch.size"));
        if (batchSize < 1)
        {
            POLL_BATCH_SIZE = 1;
        }
        else
        {
            POLL_BATCH_SIZE = Math.min(batchSize, MAX_POLL_BATCH_SIZE);
        }
    }

    private final Queue<Map.Entry<String, String>> s3PathQueue = new ConcurrentLinkedQueue<>();

    private final String queueUrl;

    private final SqsClient sqsClient;

    private final S3QS s3qs;

    private final HashSet<Integer>consumerSet;

    private final Lock lock = new ReentrantLock();

    private boolean closed = false;

    private boolean stopinput = false;

    private int invisibleTime;

    public S3Queue(S3QS s3qs, String queueUrl, int invisibleTime)
    {
        this.s3qs = s3qs;
        this.queueUrl = queueUrl;
        this.sqsClient = this.s3qs.getSqsClient();
        this.invisibleTime = invisibleTime;
        this.consumerSet = new HashSet<>();
    }
    public S3Queue(S3QS s3qs, String queueUrl)
    {

        this(s3qs, queueUrl,30);
    }

    public String getQueueUrl()
    {
        return this.queueUrl;
    }

    public int getInvisibleTime()
    {
        return this.invisibleTime;
    }


    //s3qs is the highest manager of a shuffle, so producerSet is in charge of s3qs.
    private void removeProducer(int workerId) throws IOException
    {
        this.s3qs.producerSet.remove(workerId);
        if(this.s3qs.producerSet.isEmpty())
        {
            this.stopInput();
            //this.push("");
        }
    }

    public void addConsumer(int workerId)
    {
        if(!(this.consumerSet).contains(workerId))
        {
            this.consumerSet.add(workerId);
        }
    }

    //maybe unnecessary
    public boolean removeConsumer(int workerId)
    {
        this.consumerSet.remove(workerId);
        // TODO: close queue
        return consumerSet.isEmpty();
    }

    //TODO: Implement DLQ to handle bad message.


    /**
     * Poll one object path from the SQS queue and create a physical reader for the object.
     * Calling this method can receive a batch of object paths from SQS using long polling
     * (the batch size is configured by s3qs.poll.batch.size in PIXELS_HOME/pixels.properties)
     * and add the paths into a local in-memory queue. Thus reduces the receive-message requests sent to SQS.
     *
     * @param timeoutSec the max time in seconds to wait if the queue is currently empty,
     *                   a valid wait time should be between 1 and 20 seconds
     * @return null if the queue is still empty after timeout
     * @throws IOException if fails to create the physical reader for the path
     */
    public Map.Entry<String,PhysicalReader>  poll(int timeoutSec) throws IOException, SqsException,TaskErrorException

    {
        Map.Entry<String,String> s3Path = this.s3PathQueue.poll();
        if (s3Path == null)
        {
            if (timeoutSec < 1)
            {
                timeoutSec = 1;
            }
            else
            {
                timeoutSec = Math.min(timeoutSec, MAX_POLL_WAIT_SECS);
            }
            this.lock.lock();
            try
            {
                // try poll from queue again to see if another thread has received the messages from sqs
                while ((s3Path = this.s3PathQueue.poll()) == null)
                {
                    ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .attributeNamesWithStrings("ApproximateReceiveCount")
                            .maxNumberOfMessages(POLL_BATCH_SIZE)
                            .waitTimeSeconds(timeoutSec).build();
                    ReceiveMessageResponse response = sqsClient.receiveMessage(request);
                    if (response.hasMessages())
                    {
                        for (Message message : response.messages())
                        {
                            String path = message.body();
                            String receiptHandle = message.receiptHandle();
                            this.s3PathQueue.add(new AbstractMap.SimpleEntry<>(path, receiptHandle));

                            String countStr = message.attributes().get(APPROXIMATE_RECEIVE_COUNT);
                            if (countStr == null) {
                                // 如果没有返回，可能是没有请求该属性或消息不存在该属性
                                throw new TaskErrorException("ApproximateReceiveCount not returned");
                            } else {
                                int count = Integer.parseInt(countStr);
                                // because we can only promise two receipts can be handled
                                if (count > 2) throw new TaskErrorException("Dead message occurred");
                            }
                        }
                    }
                    else
                    {
                        // the sqs queue is also empty，timeout，invoker handle this situation, which means timeout.
                        return null;
                    }
                }
            }
            finally
            {
                this.lock.unlock();
            }
        }
        String receiptHandle = s3Path.getValue();
        PhysicalReader reader =  PhysicalReaderUtil.newPhysicalReader(this.s3qs, s3Path.getKey());
        return new AbstractMap.SimpleEntry<String,PhysicalReader>(receiptHandle, reader);
    }

    // do not need to delete local queue
    public void deleteMessage(String receiptHandle) throws SqsException
    {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(this.getQueueUrl())
                .receiptHandle(receiptHandle)
                .build();
        this.sqsClient.deleteMessage(deleteMessageRequest);
    }



    protected void push(String objectPath) throws IOException
    {
        try
        {
            SendMessageRequest request = SendMessageRequest.builder()
                    .queueUrl(queueUrl).messageBody(objectPath).build();
            sqsClient.sendMessage(request);
        }catch (Exception e)
        {
            throw new IOException("sqs: fail to send message.",e);
        }
    }

    /**
     * Create a physical writer for an object of the given path. When the object is written
     * and the physical writer is closed successfully, the object path is sent to SQS.
     * @param body the information from upstream worker
     * @return the physical writer of the object
     * @throws IOException if fails to create the physical writer for the path
     */
    public PhysicalWriter offer(S3QueueMessage body) throws IOException
    {
        //TODO: same name issue
        String objectPath = getObjectPath(body);
        PhysicalS3QSWriter writer = (PhysicalS3QSWriter) PhysicalWriterUtil
                .newPhysicalWriter(this.s3qs, objectPath, false);
        writer.setQueue(this);
        if(endWork(body)) removeProducer(body.getWorkerNum());
        return writer;
    }

    private String getObjectPath(S3QueueMessage body) throws IOException
    {
        return body.getObjectPath()+body.getPartitionNum() + "/"+ String.valueOf(System.currentTimeMillis()) ;
    }

    private boolean endWork(S3QueueMessage body) throws IOException
    {
        return body.getEndWork();
    }


    public boolean isClosed()
    {
        return closed;
    }

    public void stopInput()
    {
        this.stopinput = true;
    }

    public boolean getStopInput()
    {
        return this.stopinput;
    }

    @Override
    public void close() throws IOException
    {
        this.s3PathQueue.clear();
        this.closed = true;
        // do not close the s3qs storage and the sqs client as they are cached
    }
}
