/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
public class TestPixelsWriteBuffer
{
    private List<String> columnNames = new ArrayList<>();
    private List<String> columnTypes = new ArrayList<>();
    private TypeDescription schema;
    Path targetOrderDirPath;
    Path targetCompactDirPath;
    PixelsWriteBuffer buffer;
    @Before
    public void setup()
    {
        targetOrderDirPath = new Path();
        targetOrderDirPath.setUri("file:///home/gengdy/data/tpch/1g/customer/v-0-ordered");
        targetOrderDirPath.setId(1);    // path id get from mysql `PATHS` table
        targetCompactDirPath = new Path();
        targetCompactDirPath.setUri("file:///home/gengdy/data/tpch/1g/customer/v-0-compact");
        targetCompactDirPath.setId(2);  // get from mysql `PATHS` table
        try
        {
            columnNames.add("id");
            columnNames.add("name");
            columnTypes.add("int");
            columnTypes.add("int");

            schema = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);
            buffer = new PixelsWriteBuffer(0L, schema, targetOrderDirPath, targetCompactDirPath, "localhost");  // table id get from mysql `TBLS` table
        } catch (Exception e)
        {
            System.out.println("setup error: " + e);
        }
    }
    @Test
    public void testConcurrentWriteOperations()
    {

//        // print pid if you want to attach a profiler like async-profiler or YourKit
//        try
//        {
//            System.out.println(Long.parseLong(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]));
//            Thread.sleep(10000);
//        } catch (InterruptedException e)
//        {
//            throw new RuntimeException(e);
//        }

        int numThreads = 1000;
        int numRowsPerThread = 100000;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numThreads);
        AtomicBoolean hasError = new AtomicBoolean(false);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int t = 0; t < numThreads; ++t)
        {
            final int threadId = t;
            executor.submit(() -> {
                try
                {
                    startLatch.await();
                    byte[][] values = new byte[columnTypes.size()][];
                    for (int i = 0; i < numRowsPerThread; ++i)
                    {
                        IndexProto.RowLocation.Builder builder = IndexProto.RowLocation.newBuilder();
                        values[0] = ByteBuffer.allocate(4).putInt(threadId * numRowsPerThread + i).array();
                        values[1] = ByteBuffer.allocate(4).putInt(threadId * numRowsPerThread + i + 1).array();
                        buffer.addRow(values, threadId, builder);
                    }
                } catch (Exception e)
                {
                    hasError.set(true);
                    System.out.println("error in thread " + threadId + e);
                } finally
                {
                    completionLatch.countDown();
                }
            });
        }
        startLatch.countDown();
        try
        {
            completionLatch.await();
            Thread.sleep(10000);    // wait for async flush to complete
                buffer.close();
            } catch (Exception e)
            {
                System.out.println("error: " + e);
            }
        }
    }
