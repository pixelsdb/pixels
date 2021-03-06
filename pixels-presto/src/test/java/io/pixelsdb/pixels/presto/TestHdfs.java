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
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.presto;

import io.pixelsdb.pixels.common.exception.FSException;
import io.pixelsdb.pixels.common.physical.FSFactory;
import io.pixelsdb.pixels.presto.impl.PixelsPrestoConfig;
import com.facebook.presto.spi.HostAddress;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author: tao
 * @date: Create in 2018-02-23 11:21
 **/
public class TestHdfs {

    /**
     * @ClassName: TestHdfs
     * @Title: reference: [Java访问HDFS中的数据块](https://www.cnblogs.com/zhangyinhua/p/7695700.html#_lab2_1_0)
     * @Description: reading block info from namenode
     * @param:
     * @author: tao
     * @date: 2:45PM 2018-2-23
     */
    @Test
    public void testReadBlock() throws FSException
    {
        String filePath = "hdfs://dbiir01:9000/pixels/pixels/test_105/v_2_order/201809231217040.pxl";
        PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
        FSFactory fsFactory = config.getFsFactory();
        List<LocatedBlock> allBlocks = fsFactory.listLocatedBlocks(filePath);
        for (LocatedBlock block : allBlocks) {
            ExtendedBlock eBlock = block.getBlock();
            System.out.println("BlockId: " + eBlock.getBlockId());
            System.out.println("BlockName: " + eBlock.getBlockName());
            System.out.println("BlockSize: " + block.getBlockSize());
            System.out.println(
                    block.getStartOffset());
            System.out.println(eBlock.getGenerationStamp());
            DatanodeInfo[] locations =
                    block.getLocations();
            for (DatanodeInfo info : locations) {
                System.out.println("IpAddr: " + info.getIpAddr());
                System.out.println("HostName: " + info.getHostName());
                System.out.println(info.getName());
            }
        }
    }

    // see all the datanodeInfo of one locatedBlock
    @Test
    public void testDistribute() throws FSException
    {
        PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
        String hdfsDir = "hdfs://dbiir01:9000/pixels/pixels/test_105/v_0_order";
        FSFactory fsFactory = config.getFsFactory();
        List<Path> hdfsList = fsFactory.listFiles(hdfsDir);
        Map<String, Integer> hostMap = new HashMap<>();
        for(Path hdfsPath : hdfsList){
            String filePath = hdfsPath.toString();
            List<LocatedBlock> allBlocks = fsFactory.listLocatedBlocks(filePath);
            System.out.println(hdfsPath);
            for (LocatedBlock block : allBlocks) {
                DatanodeInfo[] locations =
                        block.getLocations();
                for (DatanodeInfo info : locations) {
                    String hostname = info.getHostName();
                    if(hostMap.get(hostname) == null){
                        hostMap.put(hostname, 1);
                    }else {
                        int count = hostMap.get(hostname);
                        count++;
                        hostMap.put(hostname, count);
                    }
                    System.out.println(hostMap.keySet().toString());
                    System.out.println(hostMap.values());
                }
            }
        }
    }

    // see the first datanodeInfo of one locatedBlock
    @Test
    public void testDistributeByFirst() throws FSException
    {
        PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
        String hdfsDir = "hdfs://dbiir01:9000/pixels/pixels/test_105/v_2_order";
        FSFactory fsFactory = config.getFsFactory();
        List<Path> hdfsList = fsFactory.listFiles(hdfsDir);
        Map<String, Integer> hostMap = new HashMap<>();
        for(Path hdfsPath : hdfsList){
            String filePath = hdfsPath.toString();
            List<LocatedBlock> allBlocks = fsFactory.listLocatedBlocks(filePath);
            System.out.println(hdfsPath);
            for (LocatedBlock block : allBlocks) {
                DatanodeInfo[] locations =
                        block.getLocations();
                DatanodeInfo info = locations[0];
                String hostname = info.getHostName();
                if(hostMap.get(hostname) == null){
                    hostMap.put(hostname, 1);
                }else {
                    int count = hostMap.get(hostname);
                    count++;
                    hostMap.put(hostname, count);
                }
                System.out.println(hostMap.keySet().toString());
                System.out.println(hostMap.values());
            }
        }
    }

    @Test
    public void testListFiles() throws IOException
    {
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(configuration);
        FileStatus[] statuses = fs.listStatus(new Path("hdfs://presto00:9000/"));
        System.out.println(statuses.length);
    }


    @Test
    public void testGetFileBlocks() throws FSException
    {
        PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
        String hdfsDir = "hdfs://dbiir01:9000/pixels/pixels/test_105/v_2_order/201809231217552.pxl";
        FSFactory fsFactory = config.getFsFactory();
        List<LocatedBlock> blocks = fsFactory.listLocatedBlocks(hdfsDir);
        for (LocatedBlock block : blocks) {
            System.out.println(block.getBlock().toString() + "\n" + Arrays.asList(block.getLocations()));
            System.out.println(block.toString());
        }

        Path path = new Path(hdfsDir);
        List<HostAddress> addresses = fsFactory.getBlockLocations(path, 0, Long.MAX_VALUE);
        for (HostAddress host : addresses){
            System.out.println(host.toString());
        }
    }

    @Test
    public void testGetTime()
    {
        long start = System.nanoTime();
        System.out.println(start);
        Duration MAX_AGE = new Duration(1, TimeUnit.NANOSECONDS);
        if (Duration.nanosSince(start).compareTo(MAX_AGE) > 0) {
            System.out.println(1);
        }else
            System.out.println(0);
        long end = System.nanoTime();
        System.out.println(end);
        System.out.println(end - start);
        System.out.println(Duration.succinctNanos(end - start));
    }
}
