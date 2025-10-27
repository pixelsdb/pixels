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
package io.pixelsdb.pixels.example.core;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.CachingSinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.index.rocksdb.RocksDBIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TestCachingSinglePointIndex
{
    private CachingSinglePointIndex index;
    public final int threadNum = 16;
    public final int dataGenPerThread = 200_000;
    public final int opsPerThread = 10_000;

    public static void main(String[] args)
    {
        TestCachingSinglePointIndex test = new TestCachingSinglePointIndex();
        try
        {
            test.testRocksDBIndex();
        } catch (Exception e)
        {
            e.printStackTrace();
        } finally
        {
            try
            {
                test.tearDown();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    public void tearDown() throws IOException
    {
        if (index != null)
        {
            index.close();
            index = null;
        }
    }

    public void testRocksDBIndex() throws Exception
    {
        System.out.println("\n-- Starting benchmark for [RocksDBIndex] --");
        this.index = new RocksDBIndex(0, 0, true);
        loadData();
        updateData();
    }

    private void loadData() throws InterruptedException
    {
        System.out.println("Starting data load phase...");
        System.out.printf("Threads: %d, Entries per thread: %d%n", threadNum, dataGenPerThread);

        List<Thread> loadThreads = new ArrayList<>();
        for (int i = 0; i < threadNum; ++i)
        {
            final int threadId = i;
            loadThreads.add(new Thread(() -> {
                try
                {
                    ByteBuffer buffer = ByteBuffer.allocate(4);
                    IndexProto.IndexKey.Builder keyBuilder = IndexProto.IndexKey.newBuilder()
                            .setTableId(0)
                            .setIndexId(threadId)
                            .setTimestamp(0);

                    for (int j = 0; j < dataGenPerThread; ++j)
                    {
                        buffer.clear();
                        buffer.putInt(j);
                        ByteString keyBytes = ByteString.copyFrom(buffer.array());
                        keyBuilder.setKey(keyBytes);

                        index.putEntry(keyBuilder.build(), j);
                    }
                } catch (SinglePointIndexException e)
                {
                    throw new RuntimeException("data loading failed in thread " + threadId, e);
                }
            }));
        }

        long loadStartTime = System.currentTimeMillis();
        for (Thread t : loadThreads)
        {
            t.start();
        }

        for (Thread t : loadThreads)
        {
            t.join();
        }
        long loadEndTime = System.currentTimeMillis();

        System.out.printf("Data loading complete. Total loaded: %d entries, time: %.2fs\n",
                threadNum * dataGenPerThread, (loadEndTime - loadStartTime) / 1000.0);
    }

    private void updateData() throws InterruptedException
    {
        System.out.println("Starting performance test phase for 'updatePrimaryEntry'...");
        System.out.printf("Threads: %d, Updates per thread: %d\n", threadNum, opsPerThread);

        List<Thread> threads = new ArrayList<>();
        for(int i = 0; i < threadNum; ++i)
        {
            final int threadId = i;
            threads.add(new Thread(() -> {
                try
                {
                    ByteBuffer buffer = ByteBuffer.allocate(4);
                    IndexProto.IndexKey.Builder keyBuilder = IndexProto.IndexKey.newBuilder()
                            .setTableId(0)
                            .setIndexId(threadId);

                    for (int j = 0; j < opsPerThread; ++j)
                    {
                        int keyToUpdate = j % dataGenPerThread;
                        buffer.clear();
                        buffer.putInt(keyToUpdate);
                        ByteString keyBytes = ByteString.copyFrom(buffer.array());
                        keyBuilder.setKey(keyBytes);
                        keyBuilder.setTimestamp(System.currentTimeMillis());
                        index.updatePrimaryEntry(keyBuilder.build(), j + 1);
                    }
                } catch (SinglePointIndexException e)
                {
                    throw new RuntimeException("updatePrimaryEntry failed in thread " + threadId, e);
                }
            }));
        }

        long startTime = System.currentTimeMillis();
        for (Thread t : threads)
        {
            t.start();
        }
        for (Thread t : threads)
        {
            t.join();
        }
        long endTime = System.currentTimeMillis();

        double duration = (endTime - startTime) / 1000.0;
        long totalOps = threadNum * opsPerThread;
        double throughput = duration > 0 ? (totalOps / duration) : totalOps;

        System.out.println("--- Benchmark Summary ---");
        System.out.printf("Total update operations: %d\n", totalOps);
        System.out.printf("Total time: %.2fs\n", duration);
        System.out.printf("Throughput: %.2f ops/s\n", throughput);
        System.out.println("-------------------------");
    }
}
