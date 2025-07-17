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
package io.pixelsdb.pixels.common.index;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.EtcdException;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

public class MainIndexImplTest
{
    long tableId = 100L;
    private final MainIndexManager manager = new MainIndexManager(MainIndexImpl::new);
    MainIndex mainIndex;
    @BeforeEach
    public void setUp() throws EtcdException
    {
        EtcdUtil.Instance().deleteByPrefix("/mainindex/");
        mainIndex = manager.getOrCreate(tableId);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        mainIndex.close();
    }

    @Test
    public void testPutAndGetLocation()
    {
        long rowId = 1000L;
        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                .setFileId(1)
                .setRgId(10)
                .setRgRowId(0)
                .build();

        Assertions.assertTrue(mainIndex.putEntry(rowId, location));
        IndexProto.RowLocation fetched = mainIndex.getLocation(rowId);
        Assertions.assertNotNull(fetched);
        Assertions.assertEquals(1, fetched.getFileId());
        Assertions.assertEquals(10, fetched.getRgId());
        Assertions.assertEquals(0, fetched.getRgRowId());
    }

    @Test
    public void testDeleteEntry()
    {
        long rowId = 2000L;
        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                .setFileId(2)
                .setRgId(20)
                .setRgRowId(0)
                .build();

        Assertions.assertTrue(mainIndex.putEntry(rowId, location));
        Assertions.assertNotNull(mainIndex.getLocation(rowId));

        Assertions.assertTrue(mainIndex.deleteEntry(rowId));
        Assertions.assertNull(mainIndex.getLocation(rowId));
    }

    @Test
    public void testPutRowIdsOfRgAndDeleteRowIds()
    {
        RowIdRange range = new RowIdRange(3000L, 3004L);
        MainIndex.RgLocation location = new MainIndex.RgLocation(3, 30);

        Assertions.assertTrue(mainIndex.putRowIds(range, location));
        for (long i = 3000L; i <= 3004L; i++) {
            IndexProto.RowLocation loc = mainIndex.getLocation(i);
            Assertions.assertNotNull(loc);
            Assertions.assertEquals(3, loc.getFileId());
            Assertions.assertEquals(30, loc.getRgId());
        }

        Assertions.assertTrue(mainIndex.deleteRowIds(range));
        for (long i = 3000L; i <= 3004L; i++) {
            Assertions.assertNull(mainIndex.getLocation(i));
        }
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
                .setRgRowId(rgRowId)
                .build();
        IndexProto.RowLocation dummyLocation = IndexProto.RowLocation.newBuilder()
                .setFileId(4)
                .setRgId(40)
                .setRgRowId(0)
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

        for (int i = 0; i < threadCount; i++) {
            final int threadNum = i;
            futures.add(executor.submit(() -> {
                try {
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
                            .setRgRowId(3)
                            .build();

                    // Test putRowId()
                    IndexProto.RowLocation dummyLocation = IndexProto.RowLocation.newBuilder()
                            .setFileId(100 + threadNum)
                            .setRgId(10)
                            .setRgRowId(0)
                            .build();
                    Assertions.assertTrue(mainIndex.putEntry(rowId, dummyLocation));

                    // Test getLocation()
                    IndexProto.RowLocation fetched = mainIndex.getLocation(rowId);
                    Assertions.assertNotNull(fetched);
                    Assertions.assertEquals(100 + threadNum, fetched.getFileId());

                    // Test deleteRowId()
                    Assertions.assertTrue(mainIndex.deleteEntry(rowId));
                    Assertions.assertNull(mainIndex.getLocation(rowId));
                } finally {
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
        for (int i = 0; i < threadCount; i++) {
            long base = 10_000L + i * 100;
            ranges.add(new RowIdRange(base, base + 4));
        }

        // Concurrent putRowIdsOfRg
        for (int i = 0; i < threadCount; i++) {
            final int threadNum = i;
            futures.add(executor.submit(() -> {
                try {
                    RowIdRange range = ranges.get(threadNum);
                    MainIndex.RgLocation location = new MainIndex.RgLocation(threadNum, threadNum * 10);

                    Assertions.assertTrue(mainIndex.putRowIds(range, location));

                    for (long id = range.getStartRowId(); id <= range.getEndRowId(); id++) {
                        IndexProto.RowLocation loc = mainIndex.getLocation(id);
                        Assertions.assertNotNull(loc);
                        Assertions.assertEquals(threadNum, loc.getFileId());
                        Assertions.assertEquals(threadNum * 10, loc.getRgId());
                    }
                } finally {
                    putLatch.countDown();
                }
                return null;
            }));
        }

        // Wait for put method complete
        putLatch.await();

        // Concurrent deleteRowIdRange
        for (int i = 0; i < threadCount; i++) {
            final int threadNum = i;
            futures.add(executor.submit(() -> {
                try {
                    RowIdRange range = ranges.get(threadNum);

                    Assertions.assertTrue(mainIndex.deleteRowIds(range));
                    for (long id = range.getStartRowId(); id <= range.getEndRowId(); id++) {
                        Assertions.assertNull(mainIndex.getLocation(id));
                    }
                } finally {
                    deleteLatch.countDown();
                }
                return null;
            }));
        }

        deleteLatch.await();
        for (Future<Void> future : futures) {
            future.get();
        }
        executor.shutdown();
    }

    @Test
    public void testPersist() {
        RowIdRange range = new RowIdRange(4000L, 4002L);
        MainIndex.RgLocation location = new MainIndex.RgLocation(5, 50);

        Assertions.assertTrue(mainIndex.putRowIds(range, location));
        Assertions.assertTrue(mainIndex.persist());
    }
}