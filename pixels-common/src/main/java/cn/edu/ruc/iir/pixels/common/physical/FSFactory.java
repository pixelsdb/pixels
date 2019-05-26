/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.pixels.common.physical;

import cn.edu.ruc.iir.pixels.common.exception.FSException;
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

    public FSFactory(FileSystem fs)
    {
        this.fileSystem = fs;
    }

    private FileSystem fileSystem;
    private Configuration hdfsConfig;

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
        FileStatus[] fileStatuses = null;
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
        BlockLocation[] locations = new BlockLocation[0];
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

    // file isExist
    public boolean isTableExists(String metatable) throws IOException
    {
        Path path = new Path(metatable);
        boolean exist = fileSystem.exists(path);
        if (!exist)
        {
            fileSystem.mkdirs(path);
        }
        return exist;
    }
}
