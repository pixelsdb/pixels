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

    //TODO: need thread safe
    public final HashSet<Integer> producerSet;
    // maybe can use array to improve
    private final HashSet<Integer> PartitionSet;
    private final HashMap<Integer, S3Queue> PartitionMap;
    private final int invisibleTime;

    private SqsClient sqs;

    public S3QS()
    {
        this(30);
    }

    public S3QS(int invisibleTime){
        this.connect();
        this.producerSet = new HashSet<>();
        this.PartitionSet = new HashSet<>();
        this.PartitionMap = new HashMap<>();
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

    public void addProducer(int workerId)
    {
        if(!(this.producerSet).contains(workerId))
        {
            this.producerSet.add(workerId);
        }
    }

    //TODO: GC for files, objects and sqs.
    //TODO: allow separated invisible timeout config
    //producers in a shuffle offer their message to s3qs.
    public PhysicalWriter offer(S3QueueMessage mesg) throws IOException
    {
        S3Queue queue = null;

        //if current Partition is new one, create a new queue
        if(!(this.PartitionSet).contains(mesg.getPartitionNum()))
        {
            String queueUrl = "";
            try
            {
                queueUrl = createQueue(sqs,invisibleTime,
                        mesg.getPartitionNum()+"-"+
                                (String.valueOf(System.currentTimeMillis()))
                );
            }catch (SqsException e)
            {
                //TODO: if name is duplicated in aws try again later
                throw new IOException(e);
            }
            if(!(queueUrl.isEmpty()))
            {
                queue = openQueue(queueUrl);
                PartitionSet.add(mesg.getPartitionNum());
                PartitionMap.put(mesg.getPartitionNum(), queue);
            } else{
                throw new IOException("create new queue failed.");
            }
        }
        else
        {
            queue = PartitionMap.get(mesg.getPartitionNum());
            if(queue.getStopInput()) throw new IOException("queue " + mesg.getPartitionNum() + " is closed.");
        }
        return queue.offer(mesg);
    }

    private static String createQueue(SqsClient sqsClient,int invisibleTime, String queueName) {
        try {

            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributes(Collections.singletonMap(
                            QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(invisibleTime)
                    ))
                    .build();

            sqsClient.createQueue(createQueueRequest);

            GetQueueUrlResponse getQueueUrlResponse = sqsClient
                    .getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            return getQueueUrlResponse.queueUrl();

        } catch (SqsException e) {
            throw new RuntimeException("fail to create sqs queue: " + queueName, e);
        }
    }

    public S3Queue openQueue(String queueUrl)
    {
        return new S3Queue(this, queueUrl);
    }


    /**
    @return including writer and recipthanle(to sign a mesg in sqs).
    3 situation :closed(exception) / not ready or timeout or stopInput(null) / succeed(writer)
     */
    public Map.Entry<String,PhysicalReader> poll(S3QueueMessage mesg, int timeoutSec) throws IOException
    {
        S3Queue queue = PartitionMap.get(mesg.getPartitionNum());
        // queue close means consumer don't need to listen from it
        if(queue.isClosed()) throw new IOException("queue " + mesg.getPartitionNum() + " is closed.");
        if(queue == null) return null;

        //if there is no more input, just wait invisibletime. if timeout, no more message
        //issue: dead message maybe only handle twice, I think that's reasonable
        //TODO(OUT-OF-DATE): once a message dead, push it in dead message queue. when delete a message, delete
        Map.Entry<String,PhysicalReader> pair = null;
        try{
            if (queue.getStopInput()) {
                pair = queue.poll(queue.getInvisibleTime());
                if (pair == null) { // no more message
                    queue.close(); //logical close, no effect to consumers
                    return null; //come back later and find queue is closed
                }
            } else {
                pair = queue.poll(timeoutSec);
                if (pair == null) return null; //upstream is working, come back later
            }
        }catch (TaskErrorException e) {
            //clean up
        }

        queue.addConsumer(mesg.getWorkerNum());
        return pair;
    }

    public int finishWork(S3QueueMessage mesg) throws IOException
    {
        //DONOT close queue here. we don't know whether downstream workers meet error
        //if we check to close queue here, we maybe check many times. That is expensive
        // Once queue closed, consumers in the queue will know in the next poll.
        String receiptHandle = mesg.getReceiptHandle();
        S3Queue queue = PartitionMap.get(mesg.getPartitionNum());


        if(queue == null) {
            //queue not exist: an error, or a timeout worker
            throw new IOException("queue " + mesg.getPartitionNum() + " is closed.");
        }
        try {
            queue.deleteMessage(receiptHandle);
        }catch (SqsException e) {
            //TODO: log
            return 2;
        }

        // TODO(OUT-OF-DATE): when all the consumers exit with some messages staying in DLQ, the work occur an error
        // though we can actually close in poll() function, we close here make sure sqs can close safely.
        // thus, we can check which part of task failed in sqs.
        if(queue.removeConsumer(mesg.getWorkerNum()) && queue.isClosed())
        {
            try {
                DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                        .queueUrl(queue.getQueueUrl())
                        .build();

                sqs.deleteQueue(deleteQueueRequest);

            } catch (SqsException e) {
                // TODO： log
                return 1;
            }

        }

        return 0;
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
        for (S3Queue queue : PartitionMap.values()){
            queue.close();
        }
        this.producerSet.clear();
        this.PartitionSet.clear();
        this.PartitionMap.clear();
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
        for (S3Queue queue : PartitionMap.values()){
            queue.close();
        }
        this.producerSet.clear();
        this.PartitionSet.clear();
        this.PartitionMap.clear();
    }

    public SqsClient getSqsClient()
    {
        return sqs;
    }
}