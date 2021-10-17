/*
 * Copyright 2021 PixelsDB.
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
package io.pixelsdb.pixels.common.physical.impl;

import com.google.common.collect.ImmutableList;
import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.physical.Location;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.GenerateId;
import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.InitId;
import static io.pixelsdb.pixels.common.utils.Constants.*;
import static java.util.Objects.requireNonNull;

/**
 * <p>
 * For S3, we assume that each table is stores as one bucket.
 * And all the pixels files in this table are store as separate objects
 * in the bucket. We do not use object key prefix for performance
 * considerations.
 * </p>
 * <br/>
 * Created at: 20/08/2021
 * Author: hank
 */
public class S3 implements Storage
{
    private static Logger logger = LogManager.getLogger(S3.class);

    private static int connectionTimeoutSec = 60;
    private static int connectionAcquisitionTimeoutSec = 60;
    private static int eventLoopGroupThreads = 20;
    private static int maxConcurrentRequests = 200;
    private static int maxPendingRequests = 50_000;
    private static S3Client s3;
    private static S3AsyncClient s3Async;

    static
    {
        InitId(S3_ID_KEY);

        connectionTimeoutSec = Integer.parseInt(
                ConfigFactory.Instance().getProperty("s3.connection.timeout.sec"));
        connectionAcquisitionTimeoutSec = Integer.parseInt(
                ConfigFactory.Instance().getProperty("s3.connection.acquisition.timeout.sec"));
        eventLoopGroupThreads = Integer.parseInt(
                ConfigFactory.Instance().getProperty("s3.event.loop.group.threads"));
        maxConcurrentRequests = Integer.parseInt(
                ConfigFactory.Instance().getProperty("s3.max.concurrent.requests"));
        maxPendingRequests = Integer.parseInt(
                ConfigFactory.Instance().getProperty("s3.max.pending.requests"));

        ConfigFactory.Instance().registerUpdateCallback("s3.connection.timeout.sec", value ->
                connectionTimeoutSec = Integer.parseInt(value));
        ConfigFactory.Instance().registerUpdateCallback("s3.connection.acquisition.timeout.sec", value ->
                connectionAcquisitionTimeoutSec = Integer.parseInt(value));
        ConfigFactory.Instance().registerUpdateCallback("s3.event.loop.group.threads", value ->
                eventLoopGroupThreads = Integer.parseInt(value));
        ConfigFactory.Instance().registerUpdateCallback("s3.max.concurrent.requests", value ->
                maxConcurrentRequests = Integer.parseInt(value));
        ConfigFactory.Instance().registerUpdateCallback("s3.max.pending.requests", value ->
                maxPendingRequests = Integer.parseInt(value));
/*
        s3Async = S3AsyncClient.builder()
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(connectionTimeoutSec))
                        .putChannelOption(ChannelOption.SO_RCVBUF, 1024*1024*1024)
                        .connectionAcquisitionTimeout(Duration.ofSeconds(connectionAcquisitionTimeoutSec))
                        .eventLoopGroup(SdkEventLoopGroup.builder().numberOfThreads(eventLoopGroupThreads).build())
                        .maxConcurrency(maxConcurrentRequests).maxPendingConnectionAcquires(maxPendingRequests)).build();
*/
        s3Async = S3AsyncClient.builder()
                .httpClientBuilder(AwsCrtAsyncHttpClient.builder()
                        .maxConcurrency(maxConcurrentRequests)
                        .readBufferSize(1024 * 1024 * 1024))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofSeconds(connectionTimeoutSec))
                        .apiCallAttemptTimeout(Duration.ofSeconds(connectionAcquisitionTimeoutSec))
                        .build()).build();

        s3 = S3Client.builder().httpClientBuilder(ApacheHttpClient.builder()
                .connectionTimeout(Duration.ofSeconds(connectionTimeoutSec))
                .socketTimeout(Duration.ofSeconds(connectionTimeoutSec))
                .connectionAcquisitionTimeout(Duration.ofSeconds(connectionAcquisitionTimeoutSec))
                .maxConnections(maxConcurrentRequests)).build();
    }

    private String[] allHosts;
    private int hostIndex = 0;

    public S3()
    {
        List<KeyValue> kvs = EtcdUtil.Instance().getKeyValuesByPrefix(CACHE_NODE_STATUS_LITERAL);
        allHosts = new String[kvs.size()];
        for (int i = 0; i < kvs.size(); ++i)
        {
            String key = kvs.get(i).getKey().toString(StandardCharsets.UTF_8);
            allHosts[i] = key.substring(CACHE_NODE_STATUS_LITERAL.length());
        }
    }

    private String getPathKey(String path)
    {
        return S3_META_PREFIX + path;
    }

    private String getPathFrom(String key)
    {
        if (key.startsWith(S3_META_PREFIX))
        {
            return key.substring(S3_META_PREFIX.length());
        }
        return null;
    }

    public static class Path
    {
        public String bucket = null;
        public String key = null;
        boolean valid = false;
        boolean isBucket = false;

        public Path(String path)
        {
            requireNonNull(path);
            if (path.contains("://"))
            {
                path = path.substring(path.indexOf("://") + 3);
            }
            else if (path.startsWith("/"))
            {
                path = path.substring(1);
            }
            int slash = path.indexOf("/");
            if (slash > 0)
            {
                this.bucket = path.substring(0, slash);
                if (slash < path.length()-1)
                {
                    this.key = path.substring(slash + 1);
                }
                else
                {
                    isBucket = true;
                }
                this.valid = true;
            }
            else if (path.length() > 0)
            {
                this.bucket = path;
                this.isBucket = true;
                this.valid = true;
            }
        }

        @Override
        public String toString()
        {
            if (!this.valid)
            {
                return null;
            }
            if (this.isBucket)
            {
                return this.bucket;
            }
            return this.bucket + "/" + this.key;
        }
    }

    @Override
    public Scheme getScheme()
    {
        return Scheme.s3;
    }

    @Override
    public List<Status> listStatus(String path) throws IOException
    {
        Path p = new Path(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (!p.isBucket)
        {
            throw new IOException("Path '" + path + "' is not a directory (bucket).");
        }
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(p.bucket).build();
        ListObjectsV2Response response = s3.listObjectsV2(request);
        List<S3Object> objects = new ArrayList<>(response.keyCount());
        while (response.isTruncated())
        {
            objects.addAll(response.contents());
            request = ListObjectsV2Request.builder().bucket(p.bucket)
                    .continuationToken(response.nextContinuationToken()).build();
            response = s3.listObjectsV2(request);
        }
        objects.addAll(response.contents());
        List<Status> statuses = new ArrayList<>();
        Path op = new Path(path);
        op.isBucket = false;
        for (S3Object object : objects)
        {
            op.key = object.key();
            statuses.add(new Status(op.toString(), object.size(), false, 1));
        }
        return statuses;
    }

    @Override
    public List<String> listPaths(String path) throws IOException
    {
        return this.listStatus(path).stream().map(Status::getPath)
                .collect(Collectors.toList());
    }

    /**
     * For S3, the replication is always 1.
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public Status getStatus(String path) throws IOException
    {
        Path p = new Path(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (!this.exists(path))
        {
            throw new IOException("Path '" + path + "' does not exist.");
        }
        if (p.isBucket)
        {
            return new Status(p.toString(), 0, true, 1);
        }
        HeadObjectRequest request = HeadObjectRequest.builder().bucket(p.bucket).key(p.key).build();
        try
        {
            HeadObjectResponse response = s3.headObject(request);
            return new Status(p.toString(), response.contentLength(), false, 1);
        } catch (Exception e)
        {
            throw new IOException("Failed to get object head of '" + path + "'", e);
        }
    }

    @Override
    public long getFileId(String path) throws IOException
    {
        KeyValue kv = EtcdUtil.Instance().getKeyValue(getPathKey(path));
        if (kv == null)
        {
            // the file id does not exist, register a new id for this file.
            long id = GenerateId(S3_ID_KEY);
            EtcdUtil.Instance().putKeyValue(getPathKey(path), Long.toString(id));
            return id;
        }
        return Long.parseLong(kv.getValue().toString(StandardCharsets.UTF_8));
    }

    @Override
    public List<Location> getLocations(String path)
    {
        String host = allHosts[hostIndex++];
        if (hostIndex >= allHosts.length)
        {
            hostIndex = 0;
        }
        return ImmutableList.of(new Location(new String[]{host}));
    }

    /**
     * For S3, we do not have the concept host.
     * When S3 is used as the storage, disable enable.absolute.balancer.
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public String[] getHosts(String path) throws IOException
    {
        return allHosts;
    }

    /**
     * For S3, this open method is only used to read the data object
     * fully and sequentially. And it will load the whole object into
     * memory, so be careful for large objects.
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public DataInputStream open(String path) throws IOException
    {
        Path p = new Path(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (!this.exists(path))
        {
            throw new IOException("Path '" + path + "' does not exist.");
        }
        return new DataInputStream(new S3InputStream(s3, p.bucket, p.key));
    }

    /**
     * As S3 does not support append, we do not create object,
     * only create the file id and metadata in etcd.
     * @param path
     * @param overwrite
     * @param bufferSize
     * @param replication
     * @return
     * @throws IOException
     */
    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize, short replication) throws IOException
    {
        Path p = new Path(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (this.exists(path))
        {
            throw new IOException("Path '" + path + "' already exists.");
        }
        long id = GenerateId(S3_ID_KEY);
        EtcdUtil.Instance().putKeyValue(getPathKey(path), Long.toString(id));
        return new DataOutputStream(new S3OutputStream(s3, p.bucket, p.key));
    }

    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException
    {
        return this.create(path, overwrite, bufferSize, replication);
    }

    @Override
    public boolean delete(String path, boolean recursive) throws IOException
    {
        Path p = new Path(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (!this.exists(path))
        {
            throw new IOException("Path '" + path + "' does not exist.");
        }
        if (p.isBucket)
        {
            List<KeyValue> kvs = EtcdUtil.Instance().getKeyValuesByPrefix(getPathKey(p.bucket));
            for (KeyValue kv : kvs)
            {
                Path sub = new Path(getPathFrom(kv.getKey().toString(StandardCharsets.UTF_8)));
                DeleteObjectRequest request = DeleteObjectRequest.builder().bucket(sub.bucket).key(sub.key).build();
                try
                {
                    s3Async.deleteObject(request).get();
                } catch (Exception e)
                {
                    throw new IOException("Failed to delete object '" + sub.bucket + "/" + sub.key + "' from S3.", e);
                }
            }
        }
        else
        {
            DeleteObjectRequest request = DeleteObjectRequest.builder().bucket(p.bucket).key(p.key).build();
            try
            {
                s3Async.deleteObject(request).get();
            } catch (Exception e)
            {
                throw new IOException("Failed to delete object '" + p.bucket + "/" + p.key + "' from S3.", e);
            }
        }
        return true;
    }

    @Override
    public boolean supportDirectCopy()
    {
        return true;
    }

    @Override
    public boolean directCopy(String src, String dest) throws IOException
    {
        Path srcPath = new Path(src);
        Path destPath = new Path(dest);
        CopyObjectRequest copyReq = CopyObjectRequest.builder()
                .copySource(srcPath.toString())
                .destinationBucket(destPath.bucket)
                .destinationKey(destPath.key)
                .build();
        try
        {
            s3Async.copyObject(copyReq).join();
            return true;
        }
        catch (RuntimeException e)
        {
            throw new IOException("Failed to copy object from '" + src + "' to '" + dest + "'", e);
        }
    }

    @Override
    public void close() throws IOException
    {
        s3Async.close();
    }

    @Override
    public boolean exists(String path)
    {
        return EtcdUtil.Instance().getKeyValue(getPathKey(path)) != null;
    }

    @Override
    public boolean isFile(String path)
    {
        return !(new Path(path).isBucket);
    }

    @Override
    public boolean isDirectory(String path)
    {
        return new Path(path).isBucket;
    }

    public S3Client getClient()
    {
        return s3;
    }

    public S3AsyncClient getAsyncClient()
    {
        return s3Async;
    }
}
