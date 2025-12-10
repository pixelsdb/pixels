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
import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.storage.s3.AbstractS3;
import io.pixelsdb.pixels.storage.s3qs.io.S3QSInputStream;
import io.pixelsdb.pixels.storage.s3qs.io.S3QSOutputStream;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;

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

    public final HashSet<Integer> producerSet;
    // maybe can use array to improve
    private final HashSet<Integer> PartitionSet;
    private final HashMap<Integer, S3Queue> PartitionMap;


    private SqsClient sqs;

    public S3QS()
    {
        this.connect();
        this.producerSet = new HashSet<>();
        this.PartitionSet = new HashSet<>();
        this.PartitionMap = new HashMap<>();
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

    //producers in a shuffle offer their message to s3qs.
    public PhysicalWriter offer(S3QueueMessage mesg) throws IOException
    {
        S3Queue queue = null;

        //if current Partition is new one, create a new queue
        if(!(this.PartitionSet).contains(mesg.getPartitionNum()))
        {
            String queueUrl = "";
            try {
                queueUrl = createQueue(sqs,
                        (String.valueOf(System.currentTimeMillis()))
                        + "-" + mesg.getPartitionNum()
                );
            }catch (SqsException e) {
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
            if(queue.isClosed()) throw new IOException("queue " + mesg.getPartitionNum() + " is closed.");
        }
        return queue.offer(mesg);
    }

    private static String createQueue(SqsClient sqsClient, String queueName) {
        try {
            //System.out.println("\nCreate Queue");

            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();

            sqsClient.createQueue(createQueueRequest);

            //System.out.println("\nGet queue url");

            GetQueueUrlResponse getQueueUrlResponse = sqsClient
                    .getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            return getQueueUrlResponse.queueUrl();

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    public S3Queue openQueue(String queueUrl)
    {
        return new S3Queue(this, queueUrl);
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

    public void flush() throws IOException
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