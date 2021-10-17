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

import io.pixelsdb.pixels.common.physical.Location;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created at: 20/08/2021
 * Author: hank
 */
public class HDFS implements Storage
{
    private static Logger logger = LogManager.getLogger(HDFS.class);

    private FileSystem fs;
    private Configuration conf;

    public HDFS() throws IOException
    {
        conf = new Configuration(false);
        File configDir = new File(ConfigFactory.Instance().getProperty("hdfs.config.dir"));
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        try
        {
            if (configDir.exists() && configDir.isDirectory())
            {
                File[] hdfsConfigFiles = configDir.listFiles((file, s) -> s.endsWith("core-site.xml") || s.endsWith("hdfs-site.xml"));
                if (hdfsConfigFiles != null && hdfsConfigFiles.length == 2)
                {
                    conf.addResource(hdfsConfigFiles[0].toURI().toURL());
                    conf.addResource(hdfsConfigFiles[1].toURI().toURL());
                    logger.debug("add conf file " + hdfsConfigFiles[0].toURI() + ", " + hdfsConfigFiles[1].toURI());
                }
                else
                {
                    logger.debug("conf file not match");
                }
            }
            else
            {
                logger.error("can not read hdfs configuration file in pixels connector. hdfs.config.dir=" + configDir.getPath());
                throw new IOException("can not read hdfs configuration file from hdfs.config.dir=" + configDir.getPath());
            }
            this.fs = FileSystem.get(conf);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new IOException("I/O error occurs when reading HDFS config files.", e);
        }
    }

    public HDFS(FileSystem fs, Configuration conf)
    {
        this.fs = fs;
        this.conf = conf;
    }

    @Override
    public Scheme getScheme()
    {
        return Scheme.hdfs;
    }

    @Override
    public List<Status> listStatus(String path) throws IOException
    {
        List<Status> statuses = new ArrayList<>();
        FileStatus[] fileStatuses;
        try
        {
            fileStatuses = this.fs.listStatus(new Path(path));
            if (fileStatuses != null)
            {
                for (FileStatus f : fileStatuses)
                {
                    if (f.isFile())
                    {
                        statuses.add(new Status(f));
                    }
                }
            }
        }
        catch (IOException e)
        {
            throw new IOException("error occurs when listing files in HDFS.", e);
        }

        return statuses;
    }

    @Override
    public List<String> listPaths(String path) throws IOException
    {
        List<String> paths = new ArrayList<>();
        FileStatus[] fileStatuses;
        try
        {
            fileStatuses = this.fs.listStatus(new Path(path));
            if (fileStatuses != null)
            {
                for (FileStatus f : fileStatuses)
                {
                    if (f.isFile())
                    {
                        paths.add(f.getPath().toString());
                    }
                }
            }
        }
        catch (IOException e)
        {
            throw new IOException("error occurs when listing files in HDFS.", e);
        }

        return paths;
    }

    @Override
    public Status getStatus(String path) throws IOException
    {
        return new Status(fs.getFileStatus(new Path(path)));
    }

    @Override
    public long getFileId(String path) throws IOException
    {
        FSDataInputStream rawReader = fs.open(new Path(path));
        HdfsDataInputStream hdis;
        if (rawReader instanceof HdfsDataInputStream)
        {
            hdis = (HdfsDataInputStream) rawReader;
            try
            {
                List<LocatedBlock> locatedBlocks = hdis.getAllBlocks();
                if (locatedBlocks == null)
                {
                    throw new IOException("Failed to list blocks in HDFS file.");
                }
                if (locatedBlocks.size() != 1)
                {
                    throw new IOException("Only one block is expected per file, however there is (are) " +
                            locatedBlocks.size() + ".");
                }
                return locatedBlocks.get(0).getBlock().getBlockId();
            }
            catch (IOException e)
            {
                throw e;
            }
        }
        try
        {
            rawReader.close();
        }
        catch (IOException e)
        {
            throw e;
        }

        return -1;
    }

    @Override
    public List<Location> getLocations(String path) throws IOException
    {
        List<Location> addresses = new ArrayList<>();
        BlockLocation[] locations;
        try
        {
            locations = this.fs.getFileBlockLocations(new Path(path), 0, Long.MAX_VALUE);
        }
        catch (IOException e)
        {
            throw new IOException("I/O error occurs when getting block locations from HDFS.", e);
        }
        for (BlockLocation location : locations)
        {
            try
            {
                addresses.add(new Location(location));
            }
            catch (IOException e)
            {
                throw new IOException("I/O error occurs when get hosts and names from block locations.", e);
            }
        }
        return addresses;
    }

    @Override
    public String[] getHosts(String path) throws IOException
    {
        List<String> hosts = new ArrayList<>();
        BlockLocation[] locations;
        try
        {
            locations = this.fs.getFileBlockLocations(new Path(path), 0, Long.MAX_VALUE);
        }
        catch (IOException e)
        {
            throw new IOException("I/O error occurs when getting block locations from HDFS.", e);
        }
        for (BlockLocation location : locations)
        {
            try
            {
                hosts.addAll(Arrays.asList(location.getHosts()));
            }
            catch (IOException e)
            {
                throw new IOException("I/O error occurs when get hosts and names from block locations.", e);
            }
        }
        return hosts.toArray(new String[hosts.size()]);
    }

    @Override
    public DataInputStream open(String path) throws IOException
    {
        return fs.open(new Path(path), Constants.HDFS_BUFFER_SIZE);
    }

    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize, short replication) throws IOException
    {
        Path fsPath = new Path(path);
        return fs.create(fsPath, overwrite, bufferSize, replication, fs.getDefaultBlockSize(fsPath));
    }

    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException
    {
        return fs.create(new Path(path), overwrite, bufferSize, replication, blockSize);
    }

    @Override
    public boolean delete(String path, boolean recursive) throws IOException
    {
        return fs.delete(new Path(path), recursive);
    }

    @Override
    public boolean supportDirectCopy()
    {
        return false;
    }

    @Override
    public boolean directCopy(String src, String dest)
    {
        throw new UnsupportedOperationException("direct copy is unsupported on HDFS storage.");
    }

    @Override
    public void close() throws IOException
    {
        this.fs.close();
    }

    @Override
    public boolean exists(String path) throws IOException
    {
        return fs.exists(new Path(path));
    }

    @Override
    public boolean isFile(String path) throws IOException
    {
        return fs.getFileStatus(new Path(path)).isFile();
    }

    @Override
    public boolean isDirectory(String path) throws IOException
    {
        return fs.getFileStatus(new Path(path)).isDirectory();
    }

    public FileSystem getFileSystem()
    {
        return fs;
    }
}
