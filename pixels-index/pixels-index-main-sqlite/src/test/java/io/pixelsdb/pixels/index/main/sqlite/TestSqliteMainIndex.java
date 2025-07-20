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
package io.pixelsdb.pixels.index.main.sqlite;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.EtcdException;
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.RowIdRange;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestSqliteMainIndex
{
    long tableId = 100L;
    MainIndex mainIndex;

    @BeforeEach
    public void setUp() throws EtcdException, MainIndexException
    {
        mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        mainIndex.close();
    }

    @Test
    public void testPutAndGetLocation() throws MainIndexException
    {
        long rowId = 1000L;
        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                .setFileId(1)
                .setRgId(10)
                .setRgRowOffset(0)
                .build();

        Assertions.assertTrue(mainIndex.putEntry(rowId, location));
        IndexProto.RowLocation fetched = mainIndex.getLocation(rowId);
        Assertions.assertNotNull(fetched);
        Assertions.assertEquals(1, fetched.getFileId());
        Assertions.assertEquals(10, fetched.getRgId());
        Assertions.assertEquals(0, fetched.getRgRowOffset());
    }

    @Test
    public void testDeleteEntry() throws MainIndexException
    {
        long rowId = 2000L;
        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                .setFileId(2)
                .setRgId(20)
                .setRgRowOffset(0)
                .build();

        Assertions.assertTrue(mainIndex.putEntry(rowId, location));
        Assertions.assertNotNull(mainIndex.getLocation(rowId));
        Assertions.assertTrue(mainIndex.flushCache(2));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(rowId, rowId+1,
                2, 20, 0, 1)));
        Assertions.assertNull(mainIndex.getLocation(rowId));
    }

    @Test
    public void testGetRowId() throws Exception
    {
        long indexId = 1L;
        byte[] key = "exampleKey".getBytes();
        long timestamp = System.currentTimeMillis();
        long fileId = 1L;
        int rgId = 2;
        int rgRowId = 3;
        IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                .setIndexId(indexId)
                .setKey(ByteString.copyFrom(key))
                .setTimestamp(timestamp)
                .build();
        IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                .setFileId(fileId)
                .setRgId(rgId)
                .setRgRowOffset(rgRowId)
                .build();
        IndexProto.RowLocation dummyLocation = IndexProto.RowLocation.newBuilder()
                .setFileId(4)
                .setRgId(40)
                .setRgRowOffset(0)
                .build();
        Assertions.assertTrue(mainIndex.putEntry(0L, dummyLocation));
    }

    @Test
    public void testConcurrentAccess() throws Exception
    {
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++)
        {
            final int threadNum = i;
            futures.add(executor.submit(() -> {
                try
                {
                    // Test getRowId()
                    byte[] key = ("key-" + threadNum).getBytes();
                    long timestamp = System.currentTimeMillis();
                    long rowId = 3000L;
                    IndexProto.IndexKey keyProto = IndexProto.IndexKey.newBuilder()
                            .setIndexId(threadNum)
                            .setKey(ByteString.copyFrom(key))
                            .setTimestamp(timestamp)
                            .build();
                    IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                            .setFileId(1)
                            .setRgId(2)
                            .setRgRowOffset(3)
                            .build();

                    // Test putRowId()
                    IndexProto.RowLocation dummyLocation = IndexProto.RowLocation.newBuilder()
                            .setFileId(100 + threadNum)
                            .setRgId(10)
                            .setRgRowOffset(0)
                            .build();
                    Assertions.assertTrue(mainIndex.putEntry(rowId, dummyLocation));

                    // Test getLocation()
                    IndexProto.RowLocation fetched = mainIndex.getLocation(rowId);
                    Assertions.assertNotNull(fetched);
                    Assertions.assertEquals(100 + threadNum, fetched.getFileId());
                }
                finally
                {
                    latch.countDown();
                }
                return null;
            }));
        }

        latch.await();
        for (Future<Void> future : futures) {
            future.get();
        }
        executor.shutdown();
    }

    @Test
    public void testConcurrentPutAndDeleteRowIds() throws Exception
    {
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount * 2);

        CountDownLatch putLatch = new CountDownLatch(threadCount);
        CountDownLatch deleteLatch = new CountDownLatch(threadCount);
        List<Future<Void>> futures = new ArrayList<>();

        // Create RowIdRange for every thread
        List<RowIdRange> ranges = new ArrayList<>();
        for (int i = 0; i < threadCount; i++)
        {
            long base = 10_000L + i * 100;
            ranges.add(new RowIdRange(base, base + 4, 1L, 0, 0, 4));
        }

        // Concurrent putRowIdsOfRg
        for (int i = 0; i < threadCount; i++)
        {
            final int threadNum = i;
            futures.add(executor.submit(() -> {
                try
                {
                    RowIdRange range = ranges.get(threadNum);
                    int offset = 0;
                    for (long id = range.getRowIdStart(); id <= range.getRowIdEnd(); id++)
                    {
                        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                                .setFileId(range.getFileId()).setRgId(range.getRgId()).setRgRowOffset(offset++).build();
                        Assertions.assertTrue(mainIndex.putEntry(id, location));
                    }

                    for (long id = range.getRowIdStart(); id <= range.getRowIdEnd(); id++)
                    {
                        IndexProto.RowLocation loc = mainIndex.getLocation(id);
                        Assertions.assertNotNull(loc);
                        Assertions.assertEquals(threadNum, loc.getFileId());
                        Assertions.assertEquals(threadNum * 10, loc.getRgId());
                    }
                }
                finally
                {
                    putLatch.countDown();
                }
                return null;
            }));
        }

        // Wait for put method complete
        putLatch.await();

        // Concurrent deleteRowIdRange
        for (int i = 0; i < threadCount; i++)
        {
            final int threadNum = i;
            futures.add(executor.submit(() -> {
                try
                {
                    RowIdRange range = ranges.get(threadNum);
                    Assertions.assertTrue(mainIndex.deleteRowIdRange(range));
                    for (long id = range.getRowIdStart(); id <= range.getRowIdEnd(); id++)
                    {
                        Assertions.assertNull(mainIndex.getLocation(id));
                    }
                }
                finally
                {
                    deleteLatch.countDown();
                }
                return null;
            }));
        }

        deleteLatch.await();
        for (Future<Void> future : futures)
        {
            future.get();
        }
        executor.shutdown();
    }
}