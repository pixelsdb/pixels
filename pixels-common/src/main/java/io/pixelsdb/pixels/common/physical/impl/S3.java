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
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

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

    private static S3AsyncClient s3;

    static
    {
        InitId(S3_ID_KEY);

        S3AsyncClientBuilder builder = S3AsyncClient.builder();
        // TODO: config builder.
        s3 = builder.build();
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
            key.substring(S3_META_PREFIX.length());
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
        }
    }

    @Override
    public Scheme getScheme()
    {
        return Scheme.s3;
    }

    @Override
    public List<Status> listStatus(String path)
    {
        return null;
    }

    @Override
    public List<String> listPaths(String path) throws IOException
    {
        return null;
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
            return new Status(path, 0, true, 1);
        }
        HeadObjectRequest request = HeadObjectRequest.builder().bucket(p.bucket).key(p.key).build();
        try
        {
            HeadObjectResponse response = s3.headObject(request).get();
            return new Status(path, response.contentLength(), false, 1);
        } catch (Exception e)
        {
            throw new IOException("Failed to get object head of '" + path + "'", e);
        }
    }

    @Override
    public long getFileId(String path) throws IOException
    {
        KeyValue kv = EtcdUtil.Instance().getKeyValue(getPathKey(path));
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
        GetObjectRequest request = GetObjectRequest.builder().bucket(p.bucket).key(p.key).build();
        try
        {
            ResponseBytes<GetObjectResponse> get =
                    s3.getObject(request, AsyncResponseTransformer.toBytes()).get();
            return new DataInputStream(get.asInputStream());
        } catch (Exception e)
        {
            throw new IOException("Failed to get object '" + path + "'.", e);
        }
    }

    /**
     * As S3 does not support append, we do not create object,
     * only create the file id and metadata in etcd.
     * @param path
     * @param overwrite
     * @param bufferSize
     * @param replication
     * @return always return null.
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
        return null;
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
                    s3.deleteObject(request).get();
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
                s3.deleteObject(request).get();
            } catch (Exception e)
            {
                throw new IOException("Failed to delete object '" + p.bucket + "/" + p.key + "' from S3.", e);
            }
        }
        return true;
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

    public S3AsyncClient getClient()
    {
        return s3;
    }
}
