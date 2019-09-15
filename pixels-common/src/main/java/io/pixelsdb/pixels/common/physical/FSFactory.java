/*
 * Copyright 2018 PixelsDB.
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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.common.physical;

import io.pixelsdb.pixels.common.exception.FSException;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;

/**
 * @author hank
 * @author tao
 */
public final class FSFactory
{
    private static Logger logger = LogManager.getLogger(FSFactory.class);
    private static Map<String, FSFactory> instances = new HashMap<>();

    public static FSFactory Instance(String hdfsConfigDir) throws FSException
    {
        if (instances.containsKey(hdfsConfigDir))
        {
            return instances.get(hdfsConfigDir);
        }

        FSFactory instance = new FSFactory(hdfsConfigDir);
        instances.put(hdfsConfigDir, instance);

        return instance;
    }

    private FileSystem fileSystem;
    private Configuration hdfsConfig;

    public FSFactory(FileSystem fs)
    {
        this.fileSystem = fs;
    }

    private FSFactory(String hdfsConfigDir) throws FSException
    {
        hdfsConfig = new Configuration(false);
        File configDir = new File(hdfsConfigDir);
        hdfsConfig.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        hdfsConfig.set("fs.file.impl", LocalFileSystem.class.getName());
        try
        {
            if (configDir.exists() && configDir.isDirectory())
            {
                File[] hdfsConfigFiles = configDir.listFiles((file, s) -> s.endsWith("core-site.xml") || s.endsWith("hdfs-site.xml"));
                if (hdfsConfigFiles != null && hdfsConfigFiles.length == 2)
                {
                    hdfsConfig.addResource(hdfsConfigFiles[0].toURI().toURL());
                    hdfsConfig.addResource(hdfsConfigFiles[1].toURI().toURL());
                    logger.debug("add conf file " + hdfsConfigFiles[0].toURI() + ", " + hdfsConfigFiles[1].toURI());
                }
                else
                {
                    logger.debug("conf file not match");
                }
            }
            else
            {
                logger.error("can not read hdfs configuration file in pixels connector. hdfs.config.dir=" + hdfsConfigDir);
                throw new FSException("can not read hdfs configuration file in pixels connector. hdfs.config.dir=" + hdfsConfigDir);
            }
            this.fileSystem = FileSystem.get(hdfsConfig);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new FSException("I/O error occurs when reading HDFS config files.", e);
        }
    }

    public Optional<FileSystem> getFileSystem()
    {
        return Optional.of(this.fileSystem);
    }

    public List<Path> listFiles(Path dirPath) throws FSException
    {
        List<Path> files = new ArrayList<>();
        FileStatus[] fileStatuses;
        try
        {
            fileStatuses = this.fileSystem.listStatus(dirPath);
            if (fileStatuses != null)
            {
                for (FileStatus f : fileStatuses)
                {
                    if (f.isFile())
                    {
                        files.add(f.getPath());
                    }
                }
            }
        }
        catch (IOException e)
        {
            throw new FSException("error occurs when listing files.", e);
        }

        return files;
    }

    public List<Path> listFiles(String dirPath) throws FSException
    {
        return listFiles(new Path(dirPath));
    }

    public long getFileLength (Path path) throws IOException
    {
        if (fileSystem.isFile(path))
        {
            return fileSystem.getFileStatus(path).getLen();
        }
        return -1;
    }

    /**
     * we assume that a file contains only one block.
     *
     * @param file
     * @param start
     * @param len
     * @return
     * @throws FSException
     */
    public List<HostAddress> getBlockLocations(Path file, long start, long len) throws FSException
    {
        Set<HostAddress> addresses = new HashSet<>();
        BlockLocation[] locations;
        try
        {
            locations = this.fileSystem.getFileBlockLocations(file, start, len);
        }
        catch (IOException e)
        {
            throw new FSException("I/O error occurs when getting block locations", e);
        }
        for (BlockLocation location : locations)
        {
            try
            {
                addresses.addAll(toHostAddress(location.getHosts()));
            }
            catch (IOException e)
            {
                throw new FSException("I/O error occurs when get hosts from block locations.", e);
            }
        }
        return new ArrayList<>(addresses);
    }

    public String[] getBlockHosts(Path file, long start, long len) throws FSException
    {
        BlockLocation[] locations;
        try
        {
            locations = this.fileSystem.getFileBlockLocations(file, start, len);
        }
        catch (IOException e)
        {
            throw new FSException("I/O error occurs when getting block locations", e);
        }
        List<String> hosts = new ArrayList<>(locations.length);
        for (BlockLocation location : locations)
        {
            try
            {
                hosts.addAll(Arrays.asList(location.getHosts()));
            } catch (IOException e)
            {
                throw new FSException("I/O error occurs when get hosts from block locations.", e);
            }
        }
        return hosts.toArray(new String[hosts.size()]);
    }

    public List<HostAddress> getBlockLocations(Path file, long start, long len, String node) throws FSException
    {
        if (node == null)
            return getBlockLocations(file, start, len);
        else
        {
            ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
            builder.add(HostAddress.fromString(node));
            return builder.build();
        }
    }

    private List<HostAddress> toHostAddress(String[] hosts)
    {
        ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
        for (String host : hosts)
        {
            builder.add(HostAddress.fromString(host));
            break;
        }
        return builder.build();
    }

    public List<LocatedBlock> listLocatedBlocks(Path path) throws FSException
    {
        FSDataInputStream in = null;
        try
        {
            in = this.fileSystem.open(path);
        }
        catch (IOException e)
        {
            throw new FSException("I/O error occurs when opening file.", e);
        }
        HdfsDataInputStream hdis = (HdfsDataInputStream) in;
        List<LocatedBlock> allBlocks = null;
        try
        {
            allBlocks = hdis.getAllBlocks();
        }
        catch (IOException e)
        {
            throw new FSException("I/O error occurs when getting blocks.", e);
        }
        return allBlocks;
    }

    public List<LocatedBlock> listLocatedBlocks(String path) throws FSException
    {
        return listLocatedBlocks(new Path(path));
    }

    public void createFile(String path, String content) throws IOException
    {
        FSDataOutputStream outputStream = fileSystem.create(new Path(path));
        outputStream.write(content.getBytes());
        outputStream.close();
    }

    // write content, need to open the auth('append') in hdfs-site.xml
    public void appendContent(String hdfsPath, String content)
            throws IOException
    {
        OutputStream out = fileSystem.append(new Path(hdfsPath));
        InputStream in = new ByteArrayInputStream(content.getBytes());
        IOUtils.copyBytes(in, out, hdfsConfig);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

    public boolean isExists(String file) throws IOException
    {
        Path path = new Path(file);
        boolean exist = fileSystem.exists(path);
        if (!exist)
        {
            fileSystem.mkdirs(path);
        }
        return exist;
    }
}
