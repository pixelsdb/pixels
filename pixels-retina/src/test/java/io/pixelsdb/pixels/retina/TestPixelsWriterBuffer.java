///*
// * Copyright 2025 PixelsDB.
// *
// * This file is part of Pixels.
// *
// * Pixels is free software: you can redistribute it and/or modify
// * it under the terms of the Affero GNU General Public License as
// * published by the Free Software Foundation, either version 3 of
// * the License, or (at your option) any later version.
// *
// * Pixels is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * Affero GNU General Public License for more details.
// *
// * You should have received a copy of the Affero GNU General Public
// * License along with Pixels.  If not, see
// * <https://www.gnu.org/licenses/>.
// */
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.core.TypeDescription;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestPixelsWriterBuffer
{
    private List<String> columnNames = new ArrayList<>();
    private List<String> columnTypes = new ArrayList<>();
    private TypeDescription schema;
    String targetOrderDirPath = "file:///home/gengdy/data/test/ordered/";
    String targetCompactDirPath = "file:///home/gengdy/data/test/compact/";
    PixelsWriterBuffer buffer;

    @Before
    public void setup()
    {
        try
        {
            columnNames.add("id");
            columnNames.add("name");

            columnTypes.add("int");
            columnTypes.add("int");

            schema = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);
            buffer = new PixelsWriterBuffer(schema, "test", "test", targetOrderDirPath, targetCompactDirPath);
        } catch (Exception e)
        {
            System.out.println("setup error: " + e);
        }
    }

    @Test
    public void testConcurrentWriteOperations()
    {
        int numThreads = 100;
        int numRowsPerThread = 1000;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numThreads);
        AtomicBoolean hasError = new AtomicBoolean(false);
        AtomicInteger switchCount = new AtomicInteger(0);

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
                        values[0] = String.valueOf(threadId * numRowsPerThread + i).getBytes();
                        values[1] = String.valueOf(threadId * numRowsPerThread + i + 1).getBytes();
                        buffer.addRow(values, threadId);
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
            Thread.sleep(10000);
            buffer.close();
        } catch (Exception e)
        {
            System.out.println("error: " + e);
        }
    }
}
