/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.test;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * pixels
 *
 * @author guodong
 */
public class GetHDFSFileDistribution
{
    @Test
    public void test()
    {
        String dir = "hdfs://dbiir10:9000/pixels/pixels/test_1187/v_1_compact/";
        ConfigFactory configFactory = ConfigFactory.Instance();
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try
        {
            FileSystem fs = FileSystem.get(URI.create(configFactory.getProperty("pixels.warehouse.path")), configuration);
            FileStatus[] fileStatuses = fs.listStatus(new Path(dir));
            Map<String, Integer> hostBlockMap = new HashMap<>();
            for (FileStatus fileStatus : fileStatuses)
            {
                BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, Long.MAX_VALUE);
                for (BlockLocation blockLocation : blockLocations)
                {
                    String[] hosts = blockLocation.getHosts();
                    for (String host : hosts)
                    {
                        if (!hostBlockMap.containsKey(host))
                        {
                            hostBlockMap.put(host, 1);
                        }
                        else
                        {
                            int originV = hostBlockMap.get(host);
                            hostBlockMap.put(host, originV + 1);
                        }
                    }
                }
            }
            for (String host : hostBlockMap.keySet())
            {
                System.out.println("" + host + ":" + hostBlockMap.get(host));
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
