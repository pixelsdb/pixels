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
import java.util.HashMap;
import java.util.concurrent.*;

/**
 * @author hank
 * @create 2025-09-27
 */
public class TestS3QS
{
    private final ExecutorService executor = Executors.newFixedThreadPool(2);


    @Test
    public void TestReader() throws IOException, InterruptedException, ExecutionException
    {
        S3QS s3qs = (S3QS) StorageFactory.Instance().getStorage(Storage.Scheme.s3qs);
        Future<?> future = this.executor.submit(() -> {
            byte[] buffer = new byte[8 * 1024 * 1024];
            long startTime = System.currentTimeMillis();
            s3qs.addProducer(0); s3qs.addProducer(1);
            for (int i = 0; i < 2; i++)
            {
                S3QueueMessage body = new S3QueueMessage()
                        .setObjectPath("pixels-turbo-intermediate/shuffle/")
                        .setWorkerNum(i)
                        .setPartitionNum(99)
                        .setEndwork(true);
                try (PhysicalWriter writer = s3qs.offer(body))
                {
                    writer.append(buffer);
                    System.out.println("Wrote partition " + i);  // 添加日志
                }
                catch (IOException e)
                {
                    System.err.println("Error in iteration " + i + ": " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }

            System.out.println("write finished in " + (System.currentTimeMillis() - startTime) + " ms");

            for (int i = 0; i < 3; i++)
            {
                S3QueueMessage body = new S3QueueMessage()
                        .setObjectPath("pixels-turbo-intermediate/shuffle/")
                        .setPartitionNum(99)
                        .setWorkerNum(2+i);
                PhysicalReader reader = null;
                String receiptHandle = null;
                try{
                    HashMap.Entry<String,PhysicalReader>  pair = s3qs.poll(body,20);
                    if(pair != null) {
                        reader = pair.getValue();
                        receiptHandle = pair.getKey();
                        body.setReceiptHandle(receiptHandle);
                        System.out.println("read object " + reader.getPath());  // 添加日志
                    }
                }
                catch (IOException e)
                {
                    System.err.println("Error in iteration " + i + ": " + e.getMessage());
                    throw new RuntimeException(e);
                }
                finally {
                    if(reader != null){
                        try {
                            reader.close();
                        } catch (IOException e) {
                                throw new RuntimeException(e);
                        }
                        try {
                            s3qs.finishWork(body);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }


            System.out.println("write finished in " + (System.currentTimeMillis() - startTime) + " ms");
            try
            {
                s3qs.refresh();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });

        try {
            future.get();  // 等待任务完成并获取可能的异常
        } catch (ExecutionException e) {
            // 打印真实的异常堆栈
            //e.getCause().printStackTrace();
            throw e;
        }

        this.executor.shutdown();
        this.executor.awaitTermination(100, TimeUnit.HOURS);
    }

//    @Deprecated
//    @Test
//    public void testWriter() throws IOException, InterruptedException, ExecutionException
//    {
//        S3QS s3qs = (S3QS) StorageFactory.Instance().getStorage(Storage.Scheme.s3qs);
//        S3Queue queue = s3qs.openQueue("https://sqs.us-east-2.amazonaws.com/970089764833/pixels-shuffle");
//
//        Future<?> future = this.executor.submit(() -> {
//            byte[] buffer = new byte[8 * 1024 * 1024];
//            long startTime = System.currentTimeMillis();
//            for (int i = 0; i < 2; i++)
//            {
//                S3QueueMessage body = new S3QueueMessage()
//                        .setObjectPath("pixels-turbo-intermediate/shuffle/" + i + "/")
//                        .setPartitionNum(2)
//                        .setEndwork(false);
//                try (PhysicalWriter writer = queue.offer(body))
//                {
//                    writer.append(buffer);
//                    System.out.println("Wrote partition " + i);  // 添加日志
//                }
//                catch (IOException e)
//                {
//                    System.err.println("Error in iteration " + i + ": " + e.getMessage());
//                    throw new RuntimeException(e);
//                }
//            }
//            System.out.println("write finished in " + (System.currentTimeMillis() - startTime) + " ms");
//            try
//            {
//                queue.close();
//            }
//            catch (IOException e)
//            {
//                throw new RuntimeException(e);
//            }
//        });
//
//        try {
//            future.get();  // 等待任务完成并获取可能的异常
//        } catch (ExecutionException e) {
//            // 打印真实的异常堆栈
//            e.getCause().printStackTrace();
//            throw e;
//        }
//
//        this.executor.shutdown();
//        this.executor.awaitTermination(100, TimeUnit.HOURS);
//    }
//}

    @Test
    public void testWriter() throws IOException, InterruptedException, ExecutionException
    {
        S3QS s3qs = (S3QS) StorageFactory.Instance().getStorage(Storage.Scheme.s3qs);
        //S3Queue queue = s3qs.openQueue("https://sqs.us-east-2.amazonaws.com/970089764833/pixels-shuffle");

        Future<?> future = this.executor.submit(() -> {
            byte[] buffer = new byte[8 * 1024 * 1024];
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < 2; i++)
            {
                S3QueueMessage body = new S3QueueMessage()
                        .setObjectPath("pixels-turbo-intermediate/shuffle/" + i + "/")
                        .setPartitionNum(2)
                        .setEndwork(false);
                try (PhysicalWriter writer = s3qs.offer(body))
                {
                    writer.append(buffer);
                    System.out.println("Wrote partition " + i);  // 添加日志
                }
                catch (IOException e)
                {
                    System.err.println("Error in iteration " + i + ": " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
            System.out.println("write finished in " + (System.currentTimeMillis() - startTime) + " ms");

            try
            {
                s3qs.refresh();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });

        try {
            future.get();  // 等待任务完成并获取可能的异常
        } catch (ExecutionException e) {
            // 打印真实的异常堆栈
            //e.getCause().printStackTrace();
            throw e;
        }

        this.executor.shutdown();
        this.executor.awaitTermination(100, TimeUnit.HOURS);
    }
    @Test
    public void testMultiSQSWriter() throws IOException, InterruptedException, ExecutionException
    {
        S3QS s3qs = (S3QS) StorageFactory.Instance().getStorage(Storage.Scheme.s3qs);

        Future<?> future = this.executor.submit(() -> {
            byte[] buffer = new byte[8 * 1024 * 1024];
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < 2; i++)
            {
                S3QueueMessage body = new S3QueueMessage()
                        .setObjectPath("pixels-turbo-intermediate/shuffle/" + 9 + "/")
                        .setPartitionNum(9)
                        .setEndwork(false);
                try (PhysicalWriter writer = s3qs.offer(body))
                {
                    writer.append(buffer);
                    System.out.println("Wrote partition " + i);  // 添加日志
                }
                catch (IOException e)
                {
                    System.err.println("Error in iteration " + i + ": " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
            System.out.println("write finished in " + (System.currentTimeMillis() - startTime) + " ms");

        });

        try {
            future.get();  // 等待任务完成并获取可能的异常
        } catch (ExecutionException e) {
            // 打印真实的异常堆栈
            e.getCause().printStackTrace();
            throw e;
        }
        try
        {
            s3qs.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        this.executor.shutdown();
        this.executor.awaitTermination(100, TimeUnit.HOURS);
    }

}
