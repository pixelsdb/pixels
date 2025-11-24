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
package io.pixelsdb.pixels.index.mapdb;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author hank
 * @create 2025-11-24
 */
public class MapDBPerformanceTest
{
    private final int testDataSize;
    private DB db;
    private BTreeMap<String, String> map;

    public MapDBPerformanceTest(int testDataSize)
    {
        this.testDataSize = testDataSize;
    }

    public static void main(String[] args)
    {
        MapDBPerformanceTest test = new MapDBPerformanceTest(10000000);

        try
        {
            test.runFullPerformanceTest();

        } catch (Exception e)
        {
            e.printStackTrace();
        } finally
        {
            test.cleanup();
        }
    }

    public void initFileDB(String filePath)
    {
        db = DBMaker.fileDB(filePath)
                .fileMmapEnableIfSupported() // 启用内存映射
                .closeOnJvmShutdown()
                .make();

        map = db.treeMap("testMap")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .createOrOpen();
    }

    private Map<String, String> generateTestData()
    {
        Map<String, String> testData = new HashMap<>();
        Random random = new Random();

        for (int i = 0; i < testDataSize; i++)
        {
            String key = "key_" + i;
            String value = "value_" + random.nextInt(1000000);
            testData.put(key, value);
        }

        return testData;
    }

    public void testSingleWrite()
    {
        System.out.println("=== single point write performance ===");

        Map<String, String> testData = generateTestData();
        long startTime = System.nanoTime();

        for (Map.Entry<String, String> entry : testData.entrySet())
        {
            map.put(entry.getKey(), entry.getValue());
        }

        long endTime = System.nanoTime();
        long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

        System.out.println("write " + testDataSize + " records in " + duration + " ms");
        System.out.println("average latency: " + (double) duration / testDataSize + " ms");
        System.out.println("throughput: " + (testDataSize * 1000.0 / duration) + " ops/s");
    }

    public void testBatchWrite()
    {
        System.out.println("\n=== batch write performance ===");

        Map<String, String> testData = generateTestData();
        long startTime = System.nanoTime();

        map.putAll(testData);

        long endTime = System.nanoTime();
        long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

        System.out.println("batch write " + testDataSize + " records in " + duration + " ms");
        System.out.println("throughput: " + (testDataSize * 1000.0 / duration) + " ops/s");
    }

    public void testSingleRead()
    {
        System.out.println("\n=== single point read performance ===");

        // 先写入测试数据
        Map<String, String> testData = generateTestData();
        map.putAll(testData);

        List<String> keys = new ArrayList<>(testData.keySet());
        Collections.shuffle(keys);

        long startTime = System.nanoTime();

        for (String key : keys)
        {
            map.get(key);
        }

        long endTime = System.nanoTime();
        long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

        System.out.println(keys.size() + " single point read in " + duration + " ms");
        System.out.println("average latency: " + (double) duration / keys.size() + " ms");
        System.out.println("throughput: " + (keys.size() * 1000.0 / duration) + " ops/s");
    }

    public void testRandomRead()
    {
        System.out.println("\n=== random read performance ===");

        Random random = new Random();
        int readCount = Math.min(testDataSize, 100000); // 限制读取次数

        long startTime = System.nanoTime();

        for (int i = 0; i < readCount; i++)
        {
            String randomKey = "key_" + random.nextInt(testDataSize);
            map.get(randomKey);
        }

        long endTime = System.nanoTime();
        long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

        System.out.println(readCount + " random read in " + duration + " ms");
        System.out.println("average latency " + (double) duration / readCount + " ms");
        System.out.println("throughput: " + (readCount * 1000.0 / duration) + " ops/s");
    }

    public void testSeek()
    {
        System.out.println("\n=== seek performance ===");

        Random random = new Random();
        int readCount = Math.min(testDataSize, 100000);

        long startTime = System.nanoTime();

        for (int i = 0; i < readCount; i++)
        {
            String randomKey = "key_" + random.nextInt(testDataSize);
            map.ceilingEntry(randomKey);
        }

        long endTime = System.nanoTime();
        long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

        System.out.println(readCount + " seek in " + duration + " ms");
        System.out.println("average latency: " + (double) duration / readCount + " ms");
        System.out.println("throughput: " + (readCount * 1000.0 / duration) + " ops/s");
    }

    public void testConcurrentReadWrite() throws InterruptedException
    {
        System.out.println("\n=== concurrent read write performance ===");

        int threadCount = 10;
        int operationsPerThread = testDataSize / threadCount;

        List<Thread> threads = new ArrayList<>();

        long startTime = System.nanoTime();

        for (int i = 0; i < threadCount; i++)
        {
            final int threadId = i;
            Thread thread = new Thread(() -> {
                Random random = new Random();
                for (int j = 0; j < operationsPerThread; j++)
                {
                    if (j % 2 == 0)
                    {
                        // 写操作
                        String key = "concurrent_key_" + threadId + "_" + j;
                        String value = "concurrent_value_" + random.nextInt(1000000);
                        map.put(key, value);
                    } else
                    {
                        // 读操作
                        String key = "concurrent_key_" + threadId + "_" + (j - 1);
                        map.get(key);
                    }
                }
            });
            threads.add(thread);
            thread.start();
        }

        // 等待所有线程完成
        for (Thread thread : threads)
        {
            thread.join();
        }

        long endTime = System.nanoTime();
        long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        int totalOperations = threadCount * operationsPerThread;

        System.out.println(threadCount + " threads, " + totalOperations + " operations in " + duration + " ms");
        System.out.println("throughput: " + (totalOperations * 1000.0 / duration) + " ops/s");
    }

    public void cleanup()
    {
        if (db != null && !db.isClosed())
        {
            db.close();
        }
    }

    public void runFullPerformanceTest() throws InterruptedException
    {
        System.out.println("Begin MapDB file storage performance test, data size: " + testDataSize);
        System.out.println("==========================================");

        initFileDB("mapdb_test.db");
        runBasicTests();
        cleanup();
    }

    private void runBasicTests() throws InterruptedException
    {
        testSingleWrite();
        testBatchWrite();
        testSingleRead();
        testRandomRead();
        testSeek();
        testConcurrentReadWrite();
    }
}