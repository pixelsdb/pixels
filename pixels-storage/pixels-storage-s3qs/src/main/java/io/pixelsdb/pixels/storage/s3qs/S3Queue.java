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
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

/**
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

    private final Queue<String> s3PathQueue = new ArrayDeque<>();

    private final String queueUrl;

    private final SqsClient sqsClient;

    private final S3QS s3qs;

    private boolean closed = false;

    public S3Queue(S3QS s3qs, String queueUrl)
    {
        this.s3qs = s3qs;
        this.queueUrl = queueUrl;
        this.sqsClient = this.s3qs.getSqsClient();
    }

    /**
     * Poll one object path from the SQS queue and create a physical reader for the object.
     *
     * @param timeoutSec the max time in seconds to wait if the queue is currently empty,
     *                   a valid wait time should be between 1 and 20 seconds
     * @return null if the queue is still empty after timeout
     * @throws IOException if fails to create the physical reader for the path
     */
    public synchronized PhysicalReader poll(int timeoutSec) throws IOException
    {
        String s3Path = this.s3PathQueue.poll();
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
            ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl).maxNumberOfMessages(POLL_BATCH_SIZE).waitTimeSeconds(timeoutSec).build();
            ReceiveMessageResponse response = sqsClient.receiveMessage(request);
            if (response.hasMessages())
            {
                for (Message message : response.messages())
                {
                    String path = message.body();
                    this.s3PathQueue.add(path);
                }
                s3Path = this.s3PathQueue.poll();
            } else
            {
                return null;
            }
        }

        return PhysicalReaderUtil.newPhysicalReader(this.s3qs, s3Path);
    }

    protected void push(String objectPath)
    {
        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queueUrl).messageBody(objectPath).build();
        sqsClient.sendMessage(request);
    }

    public PhysicalWriter offer(String objectPath) throws IOException
    {
        PhysicalS3QSWriter writer = (PhysicalS3QSWriter) PhysicalWriterUtil
                .newPhysicalWriter(this.s3qs, objectPath, false);
        writer.setQueue(this);
        return writer;
    }

    public boolean isClosed()
    {
        return closed;
    }

    @Override
    public void close() throws IOException
    {
        this.s3PathQueue.clear();
        this.closed = true;
        // do not close the s3qs storage and the sqs client as they are cached
    }
}
