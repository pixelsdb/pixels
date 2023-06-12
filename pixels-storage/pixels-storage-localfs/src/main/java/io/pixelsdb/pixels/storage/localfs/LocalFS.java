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
package io.pixelsdb.pixels.storage.localfs;

import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.physical.FilePath;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.natives.DirectRandomAccessFile;
import io.pixelsdb.pixels.common.physical.natives.MappedRandomAccessFile;
import io.pixelsdb.pixels.common.physical.natives.PixelsRandomAccessFile;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.EtcdUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.GenerateId;
import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.InitId;
import static io.pixelsdb.pixels.common.utils.Constants.LOCAL_FS_ID_KEY;
import static io.pixelsdb.pixels.common.utils.Constants.LOCAL_FS_META_PREFIX;
import static java.util.Objects.requireNonNull;

/**
 * This implementation is used to access all kinds of POSIX file systems that are mounted
 * on a local directory. The file system does not need to be local physically. For example,
 * it could be a network file system mounted on a local point such as /mnt/nfs.
 *
 * @author hank
 * @create 2021-08-20
 */
public final class LocalFS implements Storage
{
    private final static boolean EnableCache;
    private final static boolean MmapEnabled;

    static
    {
        EnableCache = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("cache.enabled"));
        MmapEnabled = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("localfs.enable.mmap"));

        if (EnableCache)
        {
            /**
             * Issue #222:
             * The etcd file id is only used for cache coordination.
             * Thus, we do not initialize the id key when cache is disabled.
             */
            InitId(LOCAL_FS_ID_KEY);
        }
    }

    private static final String SchemePrefix = Scheme.file.name() + "://";

    public LocalFS() { }

    private String getPathKey(String path)
    {
        return LOCAL_FS_META_PREFIX + path;
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
    public List<Status> listStatus(String... path) throws IOException
    {
        List<Status> statuses = new ArrayList<>();
        for (String eachPath : path)
        {
            FilePath p = new FilePath(eachPath);
            if (!p.valid)
            {
                throw new IOException("Path '" + eachPath + "' is not a valid local fs path.");
            }
            File file = new File(p.realPath);
            File[] files;
            if (file.isDirectory())
            {
                files = file.listFiles();
            } else
            {
                files = new File[]{file};
            }
            if (files == null)
            {
                throw new IOException("Failed to list files in path: " + p.realPath + ".");
            } else
            {
                for (File eachFile : files)
                {
                    statuses.add(new Status(ensureSchemePrefix(eachFile.getPath()),
                            eachFile.length(), eachFile.isDirectory(), 1));
                }
            }
        }
        return statuses;
    }

    @Override
    public Status getStatus(String path) throws IOException
    {
        FilePath p = new FilePath(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not a valid local fs path.");
        }
        File file = new File(p.realPath);
        return new Status(ensureSchemePrefix(p.realPath), file.length(), file.isDirectory(), 1);
    }

    @Override
    public List<String> listPaths(String... path) throws IOException
    {
        List<String> paths = new ArrayList<>();
        for (String eachPath : path)
        {
            FilePath p = new FilePath(eachPath);
            if (!p.valid)
            {
                throw new IOException("Path '" + eachPath + "' is not a valid local fs path.");
            }
            File file = new File(p.realPath);
            File[] files;
            if (file.isDirectory())
            {
                files = file.listFiles();
            } else
            {
                files = new File[]{file};
            }
            if (files == null)
            {
                throw new IOException("Failed to list files in path: " + p.realPath + ".");
            } else
            {
                for (File eachFile : files)
                {
                    paths.add(ensureSchemePrefix(eachFile.getPath()));
                }
            }
        }
        return paths;
    }

    @Override
    public long getFileId(String path) throws IOException
    {
        requireNonNull(path, "path is null");
        if (EnableCache)
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
        else
        {
            // Issue #222: return an arbitrary id when cache is disable.
            return path.hashCode();
        }
    }

    @Override
    public boolean mkdirs(String path) throws IOException
    {
        FilePath p = new FilePath(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not a valid local fs path.");
        }
        File file = new File(p.realPath);
        if (!file.isDirectory())
        {
            throw new IOException("Path '" + p.realPath + "' is not a directory.");
        }
        if (file.exists())
        {
            throw new IOException("Directory '" + p.realPath + "' already exists.");
        }
        return file.mkdirs();
    }

    @Override
    public DataInputStream open(String path) throws IOException
    {
        FilePath p = new FilePath(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not a valid local fs path.");
        }
        File file = new File(p.realPath);
        if (file.isDirectory())
        {
            throw new IOException("Path '" + p.realPath + "' is a directory, it must be a file.");
        }
        if (!file.exists())
        {
            throw new IOException("File '" + p.realPath + "' doesn't exists.");
        }
        return new DataInputStream(Files.newInputStream(file.toPath()));
    }

    /**
     * For local fs, path is considered as local.
     *
     * @param path
     * @param overwrite
     * @param bufferSize
     * @return
     * @throws IOException if path is a directory.
     */
    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize) throws IOException
    {
        FilePath p = new FilePath(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not a valid local fs path.");
        }
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
        File parent  = file.getParentFile();
        if (parent != null)
        {
            // Issue #401: create the parent directory if not exists, ignore the return value
            parent.mkdirs();
        }
        if (!file.createNewFile())
        {
            throw new IOException("Failed to create local file '" + p.realPath + "'.");
        }
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file), bufferSize));
    }

    public PixelsRandomAccessFile openRaf(String path) throws IOException
    {
        FilePath p = new FilePath(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not a valid local fs path.");
        }
        File file = new File(p.realPath);
        if (file.isDirectory())
        {
            throw new IOException("Path '" + p.realPath + "' is a directory, it must be a file.");
        }
        if (!file.exists())
        {
            throw new IOException("File '" + p.realPath + "' doesn't exists.");
        }
        if (MmapEnabled)
        {
            return new MappedRandomAccessFile(file);
        }
        return new DirectRandomAccessFile(file);
    }

    @Override
    public boolean delete(String path, boolean recursive) throws IOException
    {
        FilePath p = new FilePath(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not a valid local fs path.");
        }
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
        /*
         * Attempt to delete the key, but it does not need to be existed.
         */
        if (EnableCache)
        {
            EtcdUtil.Instance().deleteByPrefix(getPathKey(path));
        }
        return subDeleted && file.delete();
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
        FilePath p = new FilePath(path);
        if (!p.valid)
        {
            return false;
        }
        return new File(p.realPath).exists();
    }

    @Override
    public boolean isFile(String path)
    {
        FilePath p = new FilePath(path);
        return p.valid && !p.isDir;
    }

    @Override
    public boolean isDirectory(String path)
    {
        FilePath p = new FilePath(path);
        return p.valid && p.isDir;
    }
}
