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
package io.pixelsdb.pixels.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.net.URI;
import java.nio.ByteBuffer;

/**
 * @author guodong
 */
public class TestPixelsCacheWriter
{
    @Test
    public void testSimpleCacheWriter()
    {
        try
        {
            // get fs
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            PixelsCacheConfig cacheConfig = new PixelsCacheConfig();
            FileSystem fs = FileSystem.get(URI.create(cacheConfig.getWarehousePath()), conf);
            PixelsCacheWriter cacheWriter =
                    PixelsCacheWriter.newBuilder()
                            .setCacheLocation("/home/hank/Desktop/pixels.cache")
                            .setCacheSize(1024 * 1024 * 64L)
                            .setIndexLocation("/home/hank/Desktop/pixels.index")
                            .setIndexSize(1024 * 1024 * 64L)
                            .setOverwrite(true)
                            .setFS(fs)
                            .build();
            String path = "/pixels/pixels/test_105/2121211211212.pxl";
            int index = 0;
            for (short i = 0; i < 1000; i++)
            {
                for (short j = 0; j < 64; j++)
                {
                    PixelsCacheKey cacheKey = new PixelsCacheKey(-1, i, j);
                    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
                    byteBuffer.putInt(index++);
                    byte[] value = byteBuffer.array();
                    cacheWriter.write(cacheKey, value);
                }
            }
            cacheWriter.flush();
            PixelsRadix radix = cacheWriter.getRadix();
            radix.printStats();

            PixelsRadix radix1 = PixelsCacheUtil.loadRadixIndex(cacheWriter.getIndexFile());
            radix1.printStats();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
