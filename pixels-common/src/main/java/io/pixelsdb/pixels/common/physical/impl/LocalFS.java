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

    @Override
    public Scheme getScheme()
    {
        return Scheme.file;
    }

    @Override
    public List<Status> listStatus(String path) throws IOException
    {
        File[] files = new File(path).listFiles();
        if (files == null)
        {
            throw new IOException("Failed to list files in path: " + path + ".");
        }
        else
        {
            return Stream.of(files).map(Status::new).collect(Collectors.toList());
        }
    }

    @Override
    public Status getStatus(String path)
    {
        return new Status(new File(path));
    }

    @Override
    public List<String> listPaths(String path) throws IOException
    {
        File[] files = new File(path).listFiles();
        if (files == null)
        {
            throw new IOException("Failed to list files in path: " + path + ".");
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
        File file = new File(path);
        if (file.isDirectory())
        {
            throw new IOException("Path '" + path + "' is a directory, it must be a file.");
        }
        if (!file.exists())
        {
            throw new IOException("File '" + path + "' doesn't exists.");
        }
        return new DataInputStream(new FileInputStream(file));
    }

    public RandomAccessFile openRaf(String path) throws IOException
    {
        File file = new File(path);
        if (file.isDirectory())
        {
            throw new IOException("Path '" + path + "' is a directory, it must be a file.");
        }
        if (!file.exists())
        {
            throw new IOException("File '" + path + "' doesn't exists.");
        }
        return new RandomAccessFile(file, "r");
    }

    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize, short replication) throws IOException
    {
        File file = new File(path);
        if (file.isDirectory())
        {
            throw new IOException("Path '" + path + "' is a directory, it must be a file.");
        }
        if (file.exists())
        {
            if (overwrite)
            {
                file.delete();
            }
            else
            {
                throw new IOException("File '" + path + "' already exists.");
            }
        }
        long id = GenerateId(LOCAL_FS_ID_KEY);
        EtcdUtil.Instance().putKeyValue(getPathKey(path), Long.toString(id));
        if (!file.createNewFile())
        {
            throw new IOException("Failed to create local file '" + path + "'.");
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
        File file = new File(path);
        boolean subDeleted = true;
        if (file.isDirectory())
        {
            if (!recursive)
            {
                throw new IOException("Can not delete a directory '" + path + "' with recursive = false.");
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
    public boolean exists(String path)
    {
        return new File(path).exists();
    }

    @Override
    public boolean isFile(String path)
    {
        return new File(path).isFile();
    }

    @Override
    public boolean isDirectory(String path)
    {
        return new File(path).isDirectory();
    }
}
