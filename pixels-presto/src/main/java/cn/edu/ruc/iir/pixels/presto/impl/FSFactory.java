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
package cn.edu.ruc.iir.pixels.presto.impl;

import cn.edu.ruc.iir.pixels.common.ConfigFactory;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.io.File;
import java.io.IOException;
import java.util.*;

public final class FSFactory {
    private static FileSystem FileSystem;
    private static final PixelsConfig PixelsConfig;
    private static final Logger log = Logger.get(FSFactory.class.getName());

    static
    {
        PixelsConfig = new PixelsConfig();
        Configuration hdfsConfig = new Configuration(false);
        File hdfsConfigDir = new File(ConfigFactory.Instance().getProperty("hdfs.config.dir"));
        hdfsConfig.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        hdfsConfig.set("fs.file.impl", LocalFileSystem.class.getName());
        try {
            if (hdfsConfigDir.exists() && hdfsConfigDir.isDirectory())
            {
                File[] hdfsConfigFiles = hdfsConfigDir.listFiles((file, s) -> s.endsWith("core-site.xml") || s.endsWith("hdfs-site.xml"));
                if (hdfsConfigFiles != null && hdfsConfigFiles.length == 2)
                {
                    hdfsConfig.addResource(hdfsConfigFiles[0].toURI().toURL());
                    hdfsConfig.addResource(hdfsConfigFiles[1].toURI().toURL());
                }
            }
            else
            {
                log.error("can not read hdfs configuration file in pixels connector. hdfs.config.dir=" + hdfsConfigDir);
            }

            FileSystem = FileSystem.get(hdfsConfig);
//            fileSystem.setWorkingDirectory(new Path(config.getHDFSWarehouse()));
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e);
        }
    }

    @Inject
    public FSFactory() { }

    public Optional<FileSystem> getFileSystem() {
        return Optional.of(FileSystem);
    }

    public List<Path> listFiles(Path dirPath) {
        List<Path> files = new ArrayList<>();
        FileStatus[] fileStatuses = new FileStatus[0];
        try {
            fileStatuses = FileSystem.listStatus(dirPath);
        } catch (IOException e) {
            log.error(e);
        }
        for (FileStatus f : fileStatuses) {
            if (f.isFile()) {
                files.add(f.getPath());
            }
        }
        return files;
    }

    public List<Path> listFiles(String path) {
        Path dirPath = new Path(path);
//        log.info("path: " + path);
        List<Path> files = new ArrayList<>();

        FileStatus[] fileStatuses = new FileStatus[0];
        try {
            fileStatuses = FileSystem.listStatus(dirPath);
        } catch (IOException e) {
            log.error(e);
        }
        for (FileStatus f : fileStatuses) {
            if (f.isFile()) {
                files.add(f.getPath());
            }
        }
//        log.info("files size: " + files.size());
        return files;
    }

    // file isExist
    public boolean isTableExists(String metatable) throws IOException {
        Path path = new Path(metatable);
        boolean exist = FileSystem.exists(path);
        return exist;
    }

    // assume that a file contains only a block
    public List<HostAddress> getBlockLocations(Path file, long start, long len) {
        Set<HostAddress> addresses = new HashSet<>();
        BlockLocation[] locations = new BlockLocation[0];
        try {
            locations = FileSystem.getFileBlockLocations(file, start, len);
        } catch (IOException e) {
            log.error(e);
        }
        for (BlockLocation location : locations) {
            try {
                addresses.addAll(toHostAddress(location.getHosts()));
//                log.info("FSFactory addresses: " + toHostAddress(location.getHosts()));
            } catch (IOException e) {
                log.error(e);
            }
        }
        return new ArrayList<>(addresses);
    }

    private List<HostAddress> toHostAddress(String[] hosts) {
        ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
        for (String host : hosts) {
            builder.add(HostAddress.fromString(host));
        }
        return builder.build();
    }

    private Path formPath(String dirOrFile) {
        String base = PixelsConfig.getHDFSWarehouse();
        String path = dirOrFile;
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 2);
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return Path.mergePaths(new Path(base), new Path(path));
    }

    public List<LocatedBlock> listLocatedBlocks(Path path) {
        FSDataInputStream in = null;
        try {
            in = FileSystem.open(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        HdfsDataInputStream hdis = (HdfsDataInputStream) in;
        List<LocatedBlock> allBlocks = null;
        try {
            allBlocks = hdis.getAllBlocks();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return allBlocks;
    }

    public List<LocatedBlock> listLocatedBlocks(String path) {
        return listLocatedBlocks(new Path(path));
    }

}
