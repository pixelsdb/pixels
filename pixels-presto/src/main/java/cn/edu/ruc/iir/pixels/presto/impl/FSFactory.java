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

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.util.*;

public final class FSFactory
{
    private Configuration conf = new Configuration();
    private final PixelsConfig config;
    private final Logger log = Logger.get(FSFactory.class.getName());

    @Inject
    public FSFactory(PixelsConfig config)
    {
        this.config = config;
    }

    public Optional<FileSystem> getFS()
    {
        return getFS(config.getHDFSWarehouse());
    }

    public Optional<FileSystem> getFS(String path)
    {
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        if (path.isEmpty()) {
            try {
                return Optional.of(new Path(config.getHDFSWarehouse()).getFileSystem(conf));
            }
            catch (IOException e) {
                log.error(e);
                return Optional.empty();
            }
        }
        try {
            return Optional.of(formPath(path).getFileSystem(conf));
        }
        catch (IOException e) {
            log.error(e);
            return Optional.empty();
        }
    }

    public Optional<FileSystem> getFS(Path path)
    {
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        try {
            return Optional.of(path.getFileSystem(conf));
        }
        catch (IOException e) {
            log.error(e);
            return Optional.empty();
        }
    }

    public List<Path> listFiles(Path dirPath)
    {
        List<Path> files = new ArrayList<>();
        if (!getFS().isPresent()) {
            throw new FileSystemNotFoundException("");
        }
        FileStatus[] fileStatuses = new FileStatus[0];
        try {
            fileStatuses = getFS().get().listStatus(dirPath);
        }
        catch (IOException e) {
            log.error(e);
        }
        for (FileStatus f : fileStatuses) {
            if (f.isFile()) {
                files.add(f.getPath());
            }
        }
        return files;
    }

    // assume that a file contains only a block
    public List<HostAddress> getBlockLocations(Path file, long start, long len)
    {
        Set<HostAddress> addresses = new HashSet<>();
        if (!getFS().isPresent()) {
            throw new FileSystemNotFoundException("");
        }
        BlockLocation[] locations = new BlockLocation[0];
        try {
            locations = getFS().get().getFileBlockLocations(file, start, len);
        }
        catch (IOException e) {
            log.error(e);
        }
        assert locations.length <= 1;
        for (BlockLocation location : locations) {
            try {
                addresses.addAll(toHostAddress(location.getHosts()));
            }
            catch (IOException e) {
                log.error(e);
            }
        }
        return new ArrayList<>(addresses);
    }

    private List<HostAddress> toHostAddress(String[] hosts)
    {
        ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
        for (String host : hosts) {
            builder.add(HostAddress.fromString(host));
        }
        return builder.build();
    }

    private Path formPath(String dirOrFile)
    {
        String base = config.getHDFSWarehouse();
        String path = dirOrFile;
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 2);
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return Path.mergePaths(new Path(base), new Path(path));
    }
}
