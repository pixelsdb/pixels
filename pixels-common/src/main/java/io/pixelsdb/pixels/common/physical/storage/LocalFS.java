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
package io.pixelsdb.pixels.common.physical.storage;

import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.physical.Location;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.GenerateId;
import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.InitId;
import static io.pixelsdb.pixels.common.utils.Constants.LOCAL_FS_ID_KEY;
import static io.pixelsdb.pixels.common.utils.Constants.LOCAL_FS_META_PREFIX;
import static java.util.Objects.requireNonNull;

/**
 * Created at: 20/08/2021
 * Author: hank
 */
public class LocalFS implements Storage
{
    static
    {
        InitId(LOCAL_FS_ID_KEY);
    }

    private static Logger logger = LogManager.getLogger(LocalFS.class);
    private static String SchemePrefix = Scheme.file.name() + "://";

    private String hostName;

    public LocalFS()
    {
        this.hostName = System.getenv("HOSTNAME");
        logger.debug("HostName from system env: " + hostName);
        if (hostName == null)
        {
            try
            {
                this.hostName = InetAddress.getLocalHost().getHostName();
                logger.debug("HostName from InetAddress: " + hostName);
            }
            catch (UnknownHostException e)
            {
                logger.debug("Hostname is null. Exit");
                return;
            }
        }
        logger.debug("Local FS created on host: " + hostName);
    }

    private String getPathKey(String path)
    {
        return LOCAL_FS_META_PREFIX + path + ":" + hostName;
    }

    private String getPathKeyPrefix(String path)
    {
        return LOCAL_FS_META_PREFIX + path + ":";
    }

    private String getHostFromPathKey(String pathKey)
    {
        int last = pathKey.lastIndexOf(":");
        return pathKey.substring(last + 1);
    }

    public static class Path
    {
        public String realPath = null;
        public boolean valid = false;
        public boolean isDir = false;

        public Path(String path)
        {
            requireNonNull(path);
            if (path.startsWith("file:///"))
            {
                valid = true;
                realPath = path.substring(path.indexOf("://") + 3);
            }
            else if (path.startsWith("/"))
            {
                valid = true;
                realPath = path;
            }

            if (valid)
            {
                File file = new File(realPath);
                isDir = file.isDirectory();
            }
        }

        @Override
        public String toString()
        {
            if (!this.valid)
            {
                return null;
            }
            return this.realPath;
        }
    }

    @Override
    public Scheme getScheme()
    {
        return Scheme.file;
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

    @Override
    public List<Status> listStatus(String path) throws IOException
    {
        Path p = new Path(path);
        File file = new File(p.realPath);
        File[] files = null;
        if (file.isDirectory())
        {
            files = file.listFiles();
        }
        else
        {
            files = new File[] {file};
        }
        if (files == null)
        {
            throw new IOException("Failed to list files in path: " + p.realPath + ".");
        }
        else
        {
            return Stream.of(files).map(Status::new).collect(Collectors.toList());
        }
    }

    @Override
    public Status getStatus(String path)
    {
        return new Status(new File(new Path(path).realPath));
    }

    @Override
    public List<String> listPaths(String path) throws IOException
    {
        Path p = new Path(path);
        File file = new File(p.realPath);
        File[] files = null;
        if (file.isDirectory())
        {
            files = file.listFiles();
        }
        else
        {
            files = new File[] {file};
        }
        if (files == null)
        {
            throw new IOException("Failed to list files in path: " + p.realPath + ".");
        }
        else
        {
            return Stream.of(files).map(File::getPath).collect(Collectors.toList());
        }
    }

    @Override
    public long getFileId(String path) throws IOException
    {
        KeyValue kv = EtcdUtil.Instance().getKeyValue(getPathKey(path));
        if (kv == null)
        {
            /**
             * Issue #158:
             * Create an id for this file if it does not exist in etcd.
             */
            long id = GenerateId(LOCAL_FS_ID_KEY);
            EtcdUtil.Instance().putKeyValue(getPathKey(path), Long.toString(id));
            return id;
        }
        return Long.parseLong(kv.getValue().toString(StandardCharsets.UTF_8));
    }

    @Override
    public List<Location> getLocations(String path)
    {
        List<Location> locations = new ArrayList<>();
        List<KeyValue> kvs = EtcdUtil.Instance().getKeyValuesByPrefix(getPathKeyPrefix(path));
        for (KeyValue kv : kvs)
        {
            String key = kv.getKey().toString(StandardCharsets.UTF_8);
            String host = getHostFromPathKey(key);
            locations.add(new Location(new String[]{host}));
        }
        return locations;
    }

    @Override
    public String[] getHosts(String path) throws IOException
    {
        List<KeyValue> kvs = EtcdUtil.Instance().getKeyValuesByPrefix(getPathKeyPrefix(path));
        String[] hosts = new String[kvs.size()];
        int i = 0;
        for (KeyValue kv : kvs)
        {
            String key = kv.getKey().toString(StandardCharsets.UTF_8);
            hosts[i++] = getHostFromPathKey(key);
        }
        return hosts;
    }

    @Override
    public DataInputStream open(String path) throws IOException
    {
        Path p = new Path(path);
        File file = new File(p.realPath);
        if (file.isDirectory())
        {
            throw new IOException("Path '" + p.realPath + "' is a directory, it must be a file.");
        }
        if (!file.exists())
        {
            throw new IOException("File '" + p.realPath + "' doesn't exists.");
        }
        return new DataInputStream(new FileInputStream(file));
    }

    public RandomAccessFile openRaf(String path) throws IOException
    {
        Path p = new Path(path);
        File file = new File(p.realPath);
        if (file.isDirectory())
        {
            throw new IOException("Path '" + p.realPath + "' is a directory, it must be a file.");
        }
        if (!file.exists())
        {
            throw new IOException("File '" + p.realPath + "' doesn't exists.");
        }
        return new RandomAccessFile(file, "r");
    }

    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize, short replication) throws IOException
    {
        Path p = new Path(path);
        File file = new File(p.realPath);
        if (file.isDirectory())
        {
            throw new IOException("Path '" + p.realPath + "' is a directory, it must be a file.");
        }
        if (file.exists())
        {
            if (overwrite)
            {
                file.delete();
            }
            else
            {
                throw new IOException("File '" + p.realPath + "' already exists.");
            }
        }
        long id = GenerateId(LOCAL_FS_ID_KEY);
        EtcdUtil.Instance().putKeyValue(getPathKey(path), Long.toString(id));
        if (!file.createNewFile())
        {
            throw new IOException("Failed to create local file '" + p.realPath + "'.");
        }
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file), bufferSize));
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
        File file = new File(p.realPath);
        boolean subDeleted = true;
        if (file.isDirectory())
        {
            if (!recursive)
            {
                throw new IOException("Can not delete a directory '" + p.realPath + "' with recursive = false.");
            }
            else
            {
                File[] subs = file.listFiles();

                for (File sub : subs)
                {
                    if(!delete(sub.getPath(), true))
                    {
                        subDeleted = false;
                    }
                }
            }
        }
        /**
         * Attempt to delete the key, but it does not need to be exist.
         */
        EtcdUtil.Instance().delete(getPathKey(path));
        return subDeleted && new File(path).delete();
    }

    @Override
    public boolean supportDirectCopy()
    {
        return false;
    }

    @Override
    public boolean directCopy(String src, String dest)
    {
        throw new UnsupportedOperationException("Direct copy is unsupported on LocalFS storage.");
    }

    @Override
    public void close() throws IOException { }

    @Override
    public boolean exists(String path)
    {
        return new File(new Path(path).realPath).exists();
    }

    @Override
    public boolean isFile(String path)
    {
        return !new Path(path).isDir;
    }

    @Override
    public boolean isDirectory(String path)
    {
        return new Path(path).isDir;
    }
}
