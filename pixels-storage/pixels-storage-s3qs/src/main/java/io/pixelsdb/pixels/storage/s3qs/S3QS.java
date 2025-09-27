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
import io.pixelsdb.pixels.storage.s3.AbstractS3;
import io.pixelsdb.pixels.storage.s3qs.io.S3QSInputStream;
import io.pixelsdb.pixels.storage.s3qs.io.S3QSOutputStream;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;

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

    private SqsClient sqs;

    public S3QS()
    {
        this.connect();
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
        if (this.sqs != null)
        {
            this.sqs.close();
        }
        if (s3 != null)
        {
            s3.close();
        }
    }

    public SqsClient getSqsClient()
    {
        return sqs;
    }
}