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
package io.pixelsdb.pixels.storage.s3qs;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author hank
 * @create 2025-09-27
 */
public class TestS3QS
{
    private final ExecutorService executor = Executors.newFixedThreadPool(2);

    @Test
    public void startReader() throws IOException, InterruptedException
    {
        S3QS s3qs = (S3QS) StorageFactory.Instance().getStorage(Storage.Scheme.s3qs);
        S3Queue queue = s3qs.openQueue("https://sqs.us-east-2.amazonaws.com/970089764833/pixels-shuffle");
        this.executor.submit(() -> {
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < 2; i++)
            {
                try (PhysicalReader reader = queue.poll(20))
                {
                    reader.readFully(8 * 1024 * 1024);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            System.out.println("read finished in " + (System.currentTimeMillis() - startTime) + " ms");
            try
            {
                queue.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
        this.executor.shutdown();
        this.executor.awaitTermination(100, TimeUnit.HOURS);
    }

    @Test
    public void testWriter() throws IOException, InterruptedException
    {
        S3QS s3qs = (S3QS) StorageFactory.Instance().getStorage(Storage.Scheme.s3qs);
        S3Queue queue = s3qs.openQueue("https://sqs.us-east-2.amazonaws.com/970089764833/pixels-shuffle");
        this.executor.submit(() -> {
            byte[] buffer = new byte[8 * 1024 * 1024];
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < 2; i++)
            {
                S3QueueMessage body = new S3QueueMessage()
                        .setObjectPath("pixels-turbo-intermediate/shuffle/" + i + "/")
                        .setPartitionNum(2);
                try (PhysicalWriter writer = queue.offer(body))
                {
                    writer.append(buffer);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            System.out.println("write finished in " + (System.currentTimeMillis() - startTime) + " ms");
            try
            {
                queue.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
        this.executor.shutdown();
        this.executor.awaitTermination(100, TimeUnit.HOURS);
    }
}