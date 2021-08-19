/*
 * Copyright 2017-2019 PixelsDB.
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
package io.pixelsdb.pixels.core;

import io.pixelsdb.pixels.cache.PixelsCacheConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import javax.security.sasl.AuthenticationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;

/**
 * pixels
 *
 * @author guodong
 */
public class TestHDFS
{
    @Test
    public void testScheme() throws IOException
    {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
        FileSystem fs = FileSystem.get(URI.create("hdfs://node01:9000/"), conf);
        System.out.println(fs.getScheme());
    }

    @Test
    public void testGetFileByBlockId()
            throws IOException
    {
        Configuration conf = new Configuration();
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        URLConnectionFactory connectionFactory =
                URLConnectionFactory.newDefaultURLConnectionFactory(conf);
        long blockId = 1073742421;
        long start = System.currentTimeMillis();
        String file = "";
        for (int i = 0; i < 10000; i++)
        {
            file = getFileByBlockId(ugi, connectionFactory, blockId);
        }
        long end = System.currentTimeMillis();
        System.out.println("Cost: " + (end - start));
        System.out.println(file);
    }

    private String getFileByBlockId(UserGroupInformation ugi, URLConnectionFactory connectionFactory,
                                    long blockId)
            throws IOException
    {
        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append("/fsck?ugi=").append(ugi.getShortUserName());
        urlBuilder.append("&blockId=").append("blk_").append(blockId);
        urlBuilder.insert(0, "http://localhost:50070");
        urlBuilder.append("&path=").append("%2F");
        URL path = new URL(urlBuilder.toString());
        URLConnection connection;
        try
        {
            connection = connectionFactory.openConnection(path);
        }
        catch (AuthenticationException e)
        {
            throw new IOException(e);
        }
        InputStream stream = connection.getInputStream();
        String line = null;
        String file = "";
        try (BufferedReader input = new BufferedReader(
                new InputStreamReader(stream, "UTF-8")))
        {
            while ((line = input.readLine()) != null)
            {
                if (line.startsWith("Block belongs to:"))
                {
                    file = line.split(": ")[1].trim();
                }
            }
        }
        stream.close();
        return file;
    }
}
