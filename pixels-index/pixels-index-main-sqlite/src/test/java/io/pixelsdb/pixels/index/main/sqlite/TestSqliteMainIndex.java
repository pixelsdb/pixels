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

import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.RowIdRange;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
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
    public void setUp() throws MainIndexException
    {
        // Create SQLite Directory
        try
        {
            String sqlitePath = ConfigFactory.Instance().getProperty("index.sqlite.path");
            FileUtils.forceMkdir(new File(sqlitePath));
        }
        catch (IOException e)
        {
            System.err.println("Failed to create SQLite test directory: " + e.getMessage());
        }

        mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        mainIndex.close();

        // Clear SQLite Directory
        try
        {
            String sqlitePath = ConfigFactory.Instance().getProperty("index.sqlite.path");
            FileUtils.deleteDirectory(new File(sqlitePath));
        }
        catch (IOException e)
        {
            System.err.println("Failed to clean up SQLite test directory: " + e.getMessage());
        }
    }

    @Test
    public void testPutAndGetLocation() throws MainIndexException
    {
        long rowId = 1000L;
        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                .setFileId(1).setRgId(1).setRgRowOffset(0).build();

        Assertions.assertTrue(mainIndex.putEntry(rowId, location));
        IndexProto.RowLocation fetched = mainIndex.getLocation(rowId);
        Assertions.assertNotNull(fetched);
        Assertions.assertEquals(1, fetched.getFileId());
        Assertions.assertEquals(1, fetched.getRgId());
        Assertions.assertEquals(0, fetched.getRgRowOffset());
    }

    @Test
    public void testFlushCacheAndDeleteEntry() throws MainIndexException
    {
        long rowId = 2000L;
        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                .setFileId(2).setRgId(2).setRgRowOffset(0).build();

        Assertions.assertTrue(mainIndex.putEntry(rowId, location));
        Assertions.assertNotNull(mainIndex.getLocation(rowId));
        Assertions.assertTrue(mainIndex.flushCache(2));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(rowId, rowId + 1,
                2, 2, 0, 1)));
        Assertions.assertNull(mainIndex.getLocation(rowId));

        Assertions.assertTrue(mainIndex.putEntry(rowId, location));
        Assertions.assertNotNull(mainIndex.getLocation(rowId));
        Assertions.assertTrue(mainIndex.flushCache(2));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(rowId - 1, rowId + 1,
                2, 2, 0, 2)));
        Assertions.assertNull(mainIndex.getLocation(rowId));

        Assertions.assertTrue(mainIndex.putEntry(rowId, location));
        Assertions.assertNotNull(mainIndex.getLocation(rowId));
        Assertions.assertTrue(mainIndex.flushCache(2));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(rowId - 1, rowId,
                2, 2, 0, 1)));
        Assertions.assertNotNull(mainIndex.getLocation(rowId));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(new RowIdRange(rowId, rowId + 1,
                2, 2, 0, 1)));
    }

    @Test
    public void testPutPerformance() throws MainIndexException
    {
        long rowIdBase = 0L;
        IndexProto.RowLocation.Builder locationBuilder = IndexProto.RowLocation.newBuilder()
                .setFileId(1L).setRgId(0);
        for (int i = 0; i < 10000000; i++)
        {
            mainIndex.putEntry(rowIdBase + i, locationBuilder.setRgRowOffset(i).build());
        }
        mainIndex.flushCache(1);
        mainIndex.deleteRowIdRange(new RowIdRange(
                0L, 10_000_000L, 1L, 0, 0, 10_000_000));
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
                    long rowId = 3000L;

                    // test putEntry()
                    IndexProto.RowLocation dummyLocation = IndexProto.RowLocation.newBuilder()
                            .setFileId(100 + threadNum).setRgId(10).setRgRowOffset(0).build();
                    Assertions.assertTrue(mainIndex.putEntry(rowId + threadNum, dummyLocation));

                    // test getLocation()
                    IndexProto.RowLocation fetched = mainIndex.getLocation(rowId + threadNum);
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
        for (Future<Void> future : futures)
        {
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
        // create 10 row id ranges of the same file
        for (int i = 0; i < threadCount; i++)
        {
            long base = 10_000L + i * 100;
            ranges.add(new RowIdRange(base, base + 4, i, i, 0, 4));
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
                    for (long id = range.getRowIdStart(); id < range.getRowIdEnd(); id++)
                    {
                        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                                .setFileId(range.getFileId()).setRgId(range.getRgId()).setRgRowOffset(offset++).build();
                        Assertions.assertTrue(mainIndex.putEntry(id, location));
                    }

                    for (long id = range.getRowIdStart(); id < range.getRowIdEnd(); id++)
                    {
                        IndexProto.RowLocation loc = mainIndex.getLocation(id);
                        Assertions.assertNotNull(loc);
                        Assertions.assertEquals(threadNum, loc.getFileId());
                        Assertions.assertEquals(threadNum, loc.getRgId());
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
                    mainIndex.flushCache(threadNum);
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