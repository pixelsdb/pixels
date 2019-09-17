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
package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TestParams;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestPixelsPerformance
{
    @Test
    public void testPixels()
    {
        String filePath = TestParams.filePath;
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        Path currentPath = new Path(filePath);

        try
        {
            Path path = new Path(filePath);
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            FileStatus[] fileStatuses = fs.listStatus(path);
            PixelsReader reader;
            for (FileStatus fileStatus : fileStatuses)
            {
                currentPath = fileStatus.getPath();
                reader = PixelsReaderImpl.newBuilder()
                        .setFS(fs)
                        .setPath(currentPath)
                        .build();
                PixelsReaderOption option = new PixelsReaderOption();
                String[] cols = {"UserIsStable", "UserIsCrossMarket"};
//                String[] cols = {"IsBotVNext"};
                option.skipCorruptRecords(true);
                option.tolerantSchemaEvolution(true);
                option.includeCols(cols);
                PixelsRecordReader recordReader = reader.read(option);
                int batchSize = 10000;
                VectorizedRowBatch rowBatch;
                int len = 0;
                int num = 0;
                long start = System.currentTimeMillis();
                while (true)
                {
                    rowBatch = recordReader.readBatch(batchSize);
                    String result = rowBatch.toString();
                    len += result.length();
                    if (rowBatch.endOfFile)
                    {
                        num += rowBatch.size;
                        break;
                    }
                    num += rowBatch.size;
                }
                long end = System.currentTimeMillis();
                reader.close();
                System.out.println("Path: " + fileStatus.getPath() + ", cost: " + (end - start) + ", len: " + len);
            }
        }
        catch (IOException e)
        {
            System.out.println("Err path: " + currentPath.toString());
            e.printStackTrace();
        }
    }
}
