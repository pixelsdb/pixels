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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

public class TestRGVisibility
{
    private static final int ROW_COUNT = 25600;
    private static final boolean DEBUG = true;
    private RGVisibility rgVisibility;

    @Before
    public void setUp()
    {
        rgVisibility = new RGVisibility(ROW_COUNT);
    }

    @After
    public void tearDown()
    {
        rgVisibility.close();
    }

    @Test
    public void testRGVisibilityInitialized()
    {
        long timestamp1 = 100;
        long timestamp2 = 200;

        long[] bitmap = {1, 0, 0, 0};
        RGVisibility rgVisibilityInitialized = new RGVisibility(256, 0, bitmap);

        rgVisibilityInitialized.deleteRecord(5, timestamp1);
        rgVisibilityInitialized.deleteRecord(10, timestamp1);
        rgVisibilityInitialized.deleteRecord(15, timestamp2);

        long[] bitmap1 = rgVisibilityInitialized.getVisibilityBitmap(timestamp1);
        assertEquals(0b0000010000100001L, bitmap1[0]);

        long[] bitmap2 = rgVisibilityInitialized.getVisibilityBitmap(timestamp2);
        assertEquals(0b1000010000100001L, bitmap2[0]);
    }

    @Test
    public void testBasicDeleteAndVisibility()
    {
        long timestamp1 = 100;
        long timestamp2 = 200;

        rgVisibility.deleteRecord(5, timestamp1);
        rgVisibility.deleteRecord(10, timestamp1);
         rgVisibility.deleteRecord(15, timestamp2);
        rgVisibility.garbageCollect(timestamp1);

        long[] bitmap1 = rgVisibility.getVisibilityBitmap(timestamp1);
        assertEquals(0b0000010000100000L, bitmap1[0]);

        long[] bitmap2 = rgVisibility.getVisibilityBitmap(timestamp2);
        assertEquals(0b1000010000100000L, bitmap2[0]);
    }

    @Test
    public void testMultiThread() throws InterruptedException
    {
        class DeleteRecord
        {
            final long timestamp;
            final int rowId;

            DeleteRecord(long timestamp, int rowId)
            {
                this.timestamp = timestamp;
                this.rowId = rowId;
            }
        }

        List<DeleteRecord> deleteHistory = Collections.synchronizedList(new ArrayList<>());
        Lock printLock = new ReentrantLock();
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong maxTimestamp = new AtomicLong(0);
        AtomicLong minTimestamp = new AtomicLong(0);
        AtomicInteger verificationCount = new AtomicInteger(0);

        BiConsumer<Long, long[]> verifyBitmap = (timestamp, bitmap) ->
        {
            long[] expectedBitmap = new long[bitmap.length];

            synchronized (deleteHistory)
            {
                for (DeleteRecord record : deleteHistory)
                {
                    if (record.timestamp <= timestamp)
                    {
                        int bitmapIndex = (int) (record.rowId / 64);
                        int bitOffset = (int) (record.rowId % 64);
                        expectedBitmap[bitmapIndex] |= (1L << bitOffset);
                    }
                }
            }

            for (int i = 0; i < bitmap.length; i++)
            {
                if (bitmap[i] != expectedBitmap[i])
                {
                    if (DEBUG)
                    {
                        printLock.lock();
                        try
                        {
                            System.err.printf("Bitmap verification failed at timestamp %d%n", timestamp);
                            System.err.printf("Bitmap segment %d (rows %d-%d):%n",
                                    i, i * 64, (i * 64 + 63));
                            System.err.printf("Actual:   %s%n",
                                    String.format("%64s", Long.toBinaryString(bitmap[i])).replace(' ', '0'));
                            System.err.printf("Expected: %s%n",
                                    String.format("%64s", Long.toBinaryString(expectedBitmap[i])).replace(' ', '0'));
                        } finally
                        {
                            printLock.unlock();
                        }
                    }
                    fail("Bitmap verification failed at index " + i);
                }
            }
            verificationCount.incrementAndGet();
        };

        Thread deleteThread = new Thread(() ->
        {
            long timestamp = 1;
            Random random = new Random();
            List<Integer> remainingRows = new ArrayList<>();
            for (int i = 0; i < ROW_COUNT; i++)
            {
                remainingRows.add(i);
            }

            while (!remainingRows.isEmpty() && running.get())
            {
                int index = random.nextInt(remainingRows.size());
                int rowId = remainingRows.get(index);
                remainingRows.remove(index);

                rgVisibility.deleteRecord(rowId, timestamp);
                deleteHistory.add(new DeleteRecord(timestamp, rowId));
                maxTimestamp.set(timestamp);
                timestamp++;

                try
                {
                    Thread.sleep(1);
                } catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            if (DEBUG)
            {
                printLock.lock();
                try
                {
                    System.out.printf("Delete thread completed: deleted %d rows with max timestamp %d%n",
                            deleteHistory.size(), timestamp - 1);
                } finally
                {
                    printLock.unlock();
                }
            }
            running.set(false);
        });

        Thread gcThread = new Thread(() ->
        {
            long gcTs = 0;
            while (running.get())
            {
                gcTs += 10;
                if (gcTs <= minTimestamp.get())
                {
                    rgVisibility.garbageCollect(gcTs);
                    if (DEBUG)
                    {
                        printLock.lock();
                        try
                        {
                            System.out.printf("GC thread completed: GCed up to timestamp %d%n", gcTs);
                        } finally
                        {
                            printLock.unlock();
                        }
                    }
                }
                try
                {
                    Thread.sleep(5);
                } catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        List<Thread> getThreads = new ArrayList<>();
        for (int i = 0; i < 1000; i++)
        {
            final int threadId = i;
            Thread getThread = new Thread(() ->
            {
                Random random = new Random();
                int localVerificationCount = 0;

                while (running.get())
                {
                    long maxTs = maxTimestamp.get();
                    long minTs = minTimestamp.get();
                    if (maxTs == 0 || minTs > maxTs)
                    {
                        try
                        {
                            Thread.sleep(1);
                        } catch (InterruptedException e)
                        {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        continue;
                    }

                    long queryTs = minTs + random.nextInt((int) (maxTs - minTs + 1));
                    long[] bitmap = rgVisibility.getVisibilityBitmap(queryTs);
                    verifyBitmap.accept(queryTs, bitmap);
                    localVerificationCount++;
                    minTimestamp.incrementAndGet();

                    try
                    {
                        Thread.sleep(5);
                    } catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                if (DEBUG)
                {
                    printLock.lock();
                    try
                    {
                        System.out.printf("Get thread %d completed: performed %d verifications%n",
                                threadId, localVerificationCount);
                    } finally
                    {
                        printLock.unlock();
                    }
                }
            });
            getThreads.add(getThread);
        }

        deleteThread.start();
        gcThread.start();
        getThreads.forEach(Thread::start);

        deleteThread.join();
        gcThread.join();
        for (Thread t : getThreads)
        {
            t.join();
        }

        long[] finalBitmap = rgVisibility.getVisibilityBitmap(maxTimestamp.get());
        long[] expectedFinalBitmap = new long[finalBitmap.length];
        Arrays.fill(expectedFinalBitmap, -1L);

        int invalidBitsCount = (int) (-ROW_COUNT & 255);
        if (invalidBitsCount != 0)
        {
            for (long i = ROW_COUNT; i < ROW_COUNT + invalidBitsCount; i++)
            {
                int bitmapIndex = (int) (i / 64);
                int bitOffset = (int) (i % 64);
                expectedFinalBitmap[bitmapIndex] &= ~(1L << bitOffset);
            }
        }

        verifyBitmap.accept(maxTimestamp.get(), finalBitmap);
    }
}