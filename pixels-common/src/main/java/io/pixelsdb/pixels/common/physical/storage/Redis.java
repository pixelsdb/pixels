/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.common.physical.storage;

import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.physical.io.RedisOutputStream;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import redis.clients.jedis.JedisPooled;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.InitId;
import static io.pixelsdb.pixels.common.physical.storage.AbstractS3.EnableCache;
import static io.pixelsdb.pixels.common.utils.Constants.REDIS_ID_KEY;
import static java.util.Objects.requireNonNull;

/**
 * The storage backed by Redis.
 *
 * @author hank
 * @date 8/21/22
 */
public class Redis implements Storage
{
    private static final String SchemePrefix = Scheme.redis.name() + "://";

    private static String hostName = "localhost";
    private static int port = 6379;
    private static String userName = "";
    private static String password = "";

    static
    {
        if (EnableCache)
        {
            /**
             * Issue #222:
             * The etcd file id is only used for cache coordination.
             * Thus, we do not initialize the id key when cache is disabled.
             */
            InitId(REDIS_ID_KEY);
        }
    }

    /**
     * Set the configurations for Redis. If any configuration is different from the default or
     * previous value, the Redis storage instance in StorageFactory is reloaded for the configuration
     * changes to take effect. In this case, the previous Redis storage instance acquired from the
     * StorageFactory can be used without any impact.
     * <br/>
     * If the configurations are not changed, this method is a no-op.
     *
     * @param endpoint the new endpoint of Minio
     * @param accessKey the new access key of Minio
     * @param secretKey the new secret key of Minio
     * @throws IOException
     */
    public static void ConfigRedis(String endpoint, String accessKey, String secretKey) throws IOException
    {
        requireNonNull(endpoint, "endpoint is null");
        requireNonNull(accessKey, "accessKey is null");
        requireNonNull(secretKey, "secretKey is null");

        if (endpoint.contains("://"))
        {
            endpoint = endpoint.substring(endpoint.indexOf("://") + 3);
        }
        String[] splits = endpoint.split(":");

        if (!Objects.equals(hostName, splits[0]) || port != Integer.parseInt(splits[1]))
        {
            hostName = splits[0];
            port = Integer.parseInt(splits[1]);
            userName = accessKey;
            password = secretKey;
            StorageFactory.Instance().reload(Scheme.redis);
        }
    }

    private JedisPooled jedis;

    public Redis ()
    {
        if (password == null || password.isEmpty())
        {
            checkArgument(userName == null || userName.isEmpty() || userName.equals("default"),
                    "user name must be null, empty, or 'default' when password is not defined");
            this.jedis = new JedisPooled(hostName, port);
        }
        else
        {
            this.jedis = new JedisPooled(hostName, port, userName, password);
        }
    }

    @Override
    public Scheme getScheme()
    {
        return Scheme.redis;
    }

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

    private String dropSchemePrefix(String path) throws IOException
    {
        if (path.startsWith(SchemePrefix))
        {
            return path.substring(SchemePrefix.length());
        }
        return path;
    }

    @Override
    public List<Status> listStatus(String path) throws IOException
    {
        path = dropSchemePrefix(path);
        Set<String> keys = this.jedis.keys(path + "*");
        List<Status> statuses = new ArrayList<>(keys.size());
        for (String key : keys)
        {
            long length = this.jedis.strlen(key);
            Status status = new Status(path, length, false, 1);
            statuses.add(status);
        }
        return statuses;
    }

    @Override
    public List<String> listPaths(String path) throws IOException
    {
        path = dropSchemePrefix(path);
        Set<String> keys = this.jedis.keys(path + "*");
        return new ArrayList<>(keys);
    }

    @Override
    public Status getStatus(String path) throws IOException
    {
        path = dropSchemePrefix(path);
        long length = this.jedis.strlen(path);
        if (length > 0)
        {
            Status status = new Status(path, length, false, 1);
            return status;
        }
        throw new IOException("path does not exist");
    }

    @Override
    public long getFileId(String path) throws IOException
    {
        requireNonNull(path, "path is null");
        path = dropSchemePrefix(path);
        if (EnableCache)
        {
            KeyValue kv = EtcdUtil.Instance().getKeyValue(path);
            return Long.parseLong(kv.getValue().toString(StandardCharsets.UTF_8));
        }
        else
        {
            // Issue #222: return an arbitrary id when cache is disable.
            return path.hashCode();
        }
    }

    @Override
    public boolean mkdirs(String path) throws IOException
    {
        throw new UnsupportedEncodingException("mkdirs is not supported");
    }

    @Override
    public DataInputStream open(String path) throws IOException
    {
        path = dropSchemePrefix(path);
        byte[] value = this.jedis.get(path.getBytes(StandardCharsets.UTF_8));
        if (value != null)
        {
            return new DataInputStream(new ByteArrayInputStream(value));
        }
        throw new IOException("path does not exist");
    }

    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize) throws IOException
    {
        path = dropSchemePrefix(path);
        if (this.jedis.exists(path))
        {
            if (!overwrite)
            {
                throw new IOException("Path '" + path + "' already exists.");
            }
            else
            {
                this.jedis.del(path);
            }
        }
        return new DataOutputStream(new RedisOutputStream(this.jedis, path, bufferSize));
    }

    @Override
    public boolean delete(String path, boolean recursive) throws IOException
    {
        path = dropSchemePrefix(path);
        Set<String> keys = this.jedis.keys(path + "*");
        for (String key : keys)
        {
            this.jedis.del(key);
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
        src = dropSchemePrefix(src);
        dest = dropSchemePrefix(dest);
        if (this.jedis.exists(src))
        {
            return this.jedis.copy(src, dest, true);
        }
        return false;
    }

    @Override
    public void close() throws IOException
    {
        if (this.jedis != null)
        {
            this.jedis.close();
        }
    }

    @Override
    public boolean exists(String path) throws IOException
    {
        path = dropSchemePrefix(path);
        return this.jedis.exists(path);
    }

    @Override
    public boolean isFile(String path) throws IOException
    {
        return true;
    }

    @Override
    public boolean isDirectory(String path) throws IOException
    {
        return false;
    }

    public JedisPooled getJedis()
    {
        return this.jedis;
    }
}
