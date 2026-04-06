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
        rgVisibility = new RGVisibility(ROW_COUNT, 0L, null);
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

        // Probe the native library to determine per-tile bitmap size,
        // which depends on RETINA_CAPACITY set at compile time.
        int bitmapWords;
        try (RGVisibility probe = new RGVisibility(1, 0L, null))
        {
            bitmapWords = probe.getVisibilityBitmap(0).length;
        }

        long[] bitmap = new long[bitmapWords];
        bitmap[0] = 1;
        RGVisibility rgVisibilityInitialized = new RGVisibility(256, 0, bitmap);

        rgVisibilityInitialized.deleteRecord(5, timestamp1);
        rgVisibilityInitialized.deleteRecord(10, timestamp1);
        rgVisibilityInitialized.deleteRecord(15, timestamp2);

        long[] bitmap1 = rgVisibilityInitialized.getVisibilityBitmap(timestamp1);
        assertEquals(0b0000010000100001L, bitmap1[0]);

        long[] bitmap2 = rgVisibilityInitialized.getVisibilityBitmap(timestamp2);
        assertEquals(0b1000010000100001L, bitmap2[0]);

        rgVisibilityInitialized.close();
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
    public void testMemoryUsage()
    {
        // 1. Capture initial state
        long initialNative = RGVisibility.getMemoryUsage();
        long initialTracked = RGVisibility.getTrackedMemoryUsage();
        long initialObjects = RGVisibility.getRetinaTrackedObjectCount();

        int testCount = 10000000; // 10 Million deletions

        // 2. Perform intensive delete operations
        for (int i = 0; i < testCount; i++)
        {
            rgVisibility.deleteRecord(i % ROW_COUNT, 1000 + i);
        }

        long afterDeletesNative = RGVisibility.getMemoryUsage();
        long afterDeletesTracked = RGVisibility.getTrackedMemoryUsage();
        long afterDeletesObjects = RGVisibility.getRetinaTrackedObjectCount();

        // Calculate business logic growth
        long netObjects = afterDeletesObjects - initialObjects;
        long netTrackedBytes = afterDeletesTracked - initialTracked;

        // Calculate pure memory (Excluding 8-byte vptr per object)
        long vptrTotalOverhead = netObjects * 8;
        long pureBusinessMemory = netTrackedBytes - vptrTotalOverhead;

        if (DEBUG) {
            System.out.println("--- Post-Deletion Audit ---");
            System.out.println("New Objects Created: " + netObjects);
            System.out.println("Tracked Memory (incl. vptr): " + netTrackedBytes + " bytes");
            System.out.println("Pure Business Memory (excl. vptr): " + pureBusinessMemory + " bytes");

            if (netObjects > 0) {
                System.out.println(String.format("Average Pure Size per Object: %.2f bytes",
                        (double) pureBusinessMemory / netObjects));
            }
        }

        // Assertion: Ensure objects are actually tracked
        assertTrue("Object count should increase", netObjects > 0);

        // 3. Perform Garbage Collection
        // Use a timestamp that covers all previous operations
        rgVisibility.garbageCollect(100000000 + testCount + 100);

        long finalNative = RGVisibility.getMemoryUsage();
        long finalTracked = RGVisibility.getTrackedMemoryUsage();
        long finalObjects = RGVisibility.getRetinaTrackedObjectCount();

        if (DEBUG) {
            System.out.println("--- Post-GC Audit ---");
            System.out.println("Residual Objects: " + finalObjects);
            System.out.println("Residual Tracked Memory: " + finalTracked + " bytes");
        }

        // 4. Critical Assertions for Paper Accuracy
        // The object count must return to baseline if there is no leak
        assertEquals("Object leak detected! Object count should return to baseline.",
                initialObjects, finalObjects, 10); // Allowing small constant overhead

        // Tracked memory should also return to baseline
        assertTrue("Tracked memory should be reclaimed", finalTracked < afterDeletesTracked);

        // Native memory (jemalloc) should follow the trend
        assertTrue("Physical memory (jemalloc) should decrease after GC", finalNative < afterDeletesNative);
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

    // =====================================================================
    // gcSnapshotBitmap JNI round-trip tests
    //
    // Verify that garbageCollect() (now returning long[]) produces a bitmap
    // identical to getVisibilityBitmap() called BEFORE GC — the independent
    // ground truth computed from the full, unmodified deletion chain.
    // =====================================================================

    private static void assertBitmapsEqual(String msg, long[] expected, long[] actual)
    {
        assertEquals(msg + ": length mismatch", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++)
        {
            if (expected[i] != actual[i])
            {
                fail(String.format("%s: word %d (rows %d-%d) mismatch%n  expected: %s%n  actual:   %s",
                        msg, i, i * 64, i * 64 + 63,
                        String.format("%64s", Long.toBinaryString(expected[i])).replace(' ', '0'),
                        String.format("%64s", Long.toBinaryString(actual[i])).replace(' ', '0')));
            }
        }
    }

    @Test
    public void testGcSnapshotEarlyReturnA()
    {
        // Empty chain → all-zero snapshot
        long[] preRef0 = rgVisibility.getVisibilityBitmap(100);
        long[] snap0 = rgVisibility.garbageCollect(100);
        assertBitmapsEqual("empty chain", preRef0, snap0);
        for (long w : snap0)
        {
            assertEquals(0L, w);
        }

        // Add deletes and compact to advance baseTimestamp
        rgVisibility.deleteRecord(5, 100);
        rgVisibility.deleteRecord(10, 100);
        rgVisibility.deleteRecord(15, 200);

        // First GC at ts=200 → compact all items
        long[] preRef1 = rgVisibility.getVisibilityBitmap(200);
        long[] snap1 = rgVisibility.garbageCollect(200);
        assertBitmapsEqual("first compact", preRef1, snap1);

        // Second GC at ts=200 → early return A (ts == baseTimestamp)
        long[] preRef2 = rgVisibility.getVisibilityBitmap(200);
        long[] snap2 = rgVisibility.garbageCollect(200);
        assertBitmapsEqual("repeat GC", preRef2, snap2);

        // Both snapshots must be identical
        assertBitmapsEqual("snap1 vs snap2", snap1, snap2);
    }

    @Test
    public void testGcSnapshotEarlyReturnB()
    {
        // 5 items in one block: ts 1,2,3,8,10.  Block last ts=10 > safeGcTs=5
        rgVisibility.deleteRecord(0, 1);
        rgVisibility.deleteRecord(1, 2);
        rgVisibility.deleteRecord(2, 3);
        rgVisibility.deleteRecord(3, 8);
        rgVisibility.deleteRecord(4, 10);

        long[] preRef = rgVisibility.getVisibilityBitmap(5);
        long[] snapshot = rgVisibility.garbageCollect(5);
        assertBitmapsEqual("early return B", preRef, snapshot);

        // Rows 0,1,2 marked (ts ≤ 5); rows 3,4 not
        assertEquals(0b111L, snapshot[0]);
    }

    @Test
    public void testGcSnapshotCompactWithBoundary()
    {
        // 10 items: rows 0-9, ts 1-10
        // Block 1 (8 items, ts 1-8): compactable at safeGcTs=9
        // Block 2 (2 items, ts 9-10): boundary block (tail)
        for (int i = 0; i < 10; i++)
        {
            rgVisibility.deleteRecord(i, i + 1);
        }

        long[] preRef = rgVisibility.getVisibilityBitmap(9);
        long[] snapshot = rgVisibility.garbageCollect(9);
        assertBitmapsEqual("compact with boundary", preRef, snapshot);

        // Rows 0-8 marked, row 9 not
        assertEquals(0x1FFL, snapshot[0]);
    }

    @Test
    public void testGcSnapshotCompactAllBlocks()
    {
        // 8 items fill one block: rows 0-7, ts 1-8
        for (int i = 0; i < 8; i++)
        {
            rgVisibility.deleteRecord(i, i + 1);
        }

        // safeGcTs=10 > all item ts → entire block compacted
        long[] preRef = rgVisibility.getVisibilityBitmap(10);
        long[] snapshot = rgVisibility.garbageCollect(10);
        assertBitmapsEqual("compact all blocks", preRef, snapshot);

        assertEquals(0xFFL, snapshot[0]);
    }

    @Test
    public void testGcSnapshotCompactMultiBlock()
    {
        // 20 items: rows 0-19, ts 1-20
        // Block 1 (ts 1-8), Block 2 (ts 9-16), Block 3 tail (ts 17-20)
        for (int i = 0; i < 20; i++)
        {
            rgVisibility.deleteRecord(i, i + 1);
        }

        // safeGcTs=18: blocks 1,2 compacted, block 3 is boundary
        long[] preRef = rgVisibility.getVisibilityBitmap(18);
        long[] snapshot = rgVisibility.garbageCollect(18);
        assertBitmapsEqual("compact multi-block", preRef, snapshot);

        // Rows 0-17 marked
        assertEquals((1L << 18) - 1, snapshot[0]);
    }

    @Test
    public void testGcSnapshotCrossTile()
    {
        // Tile 0: rows 0-255    Tile 1: rows 256-511    Tile 2: rows 512-767
        rgVisibility.deleteRecord(5, 1);
        rgVisibility.deleteRecord(10, 2);
        rgVisibility.deleteRecord(260, 3);   // tile 1
        rgVisibility.deleteRecord(600, 4);   // tile 2
        rgVisibility.deleteRecord(100, 5);   // tile 0
        rgVisibility.deleteRecord(300, 6);   // tile 1

        long[] preRef1 = rgVisibility.getVisibilityBitmap(4);
        long[] snap1 = rgVisibility.garbageCollect(4);
        assertBitmapsEqual("cross-tile ts=4", preRef1, snap1);

        long[] preRef2 = rgVisibility.getVisibilityBitmap(6);
        long[] snap2 = rgVisibility.garbageCollect(6);
        assertBitmapsEqual("cross-tile ts=6", preRef2, snap2);
    }

    @Test
    public void testGcSnapshotProgressiveRounds()
    {
        // Phase 1: 20 deletes at ts 1-20
        for (int i = 0; i < 20; i++)
        {
            rgVisibility.deleteRecord(i, i + 1);
        }

        long[] preRef1 = rgVisibility.getVisibilityBitmap(5);
        long[] snap1 = rgVisibility.garbageCollect(5);
        assertBitmapsEqual("round 1", preRef1, snap1);

        long[] preRef2 = rgVisibility.getVisibilityBitmap(12);
        long[] snap2 = rgVisibility.garbageCollect(12);
        assertBitmapsEqual("round 2", preRef2, snap2);

        // Phase 2: 10 more deletes at ts 21-30
        for (int i = 20; i < 30; i++)
        {
            rgVisibility.deleteRecord(i, i + 1);
        }

        long[] preRef3 = rgVisibility.getVisibilityBitmap(25);
        long[] snap3 = rgVisibility.garbageCollect(25);
        assertBitmapsEqual("round 3", preRef3, snap3);

        long[] preRef4 = rgVisibility.getVisibilityBitmap(100);
        long[] snap4 = rgVisibility.garbageCollect(100);
        assertBitmapsEqual("round 4", preRef4, snap4);

        // All 30 rows marked
        assertEquals((1L << 30) - 1, snap4[0]);
    }

    @Test
    public void testGcSnapshotRandomized()
    {
        Random rng = new Random(42);
        boolean[] deleted = new boolean[ROW_COUNT];
        long ts = 1;
        long lastGcTs = 0;

        for (int round = 0; round < 10; round++)
        {
            for (int d = 0; d < 100; d++)
            {
                int rowId;
                do { rowId = rng.nextInt(ROW_COUNT); } while (deleted[rowId]);
                deleted[rowId] = true;
                rgVisibility.deleteRecord(rowId, ts);
                ts++;
            }

            long gcTs = lastGcTs + 51;
            if (gcTs >= ts) gcTs = ts - 1;

            long[] preRef = rgVisibility.getVisibilityBitmap(gcTs);
            long[] snapshot = rgVisibility.garbageCollect(gcTs);
            assertBitmapsEqual("randomized round " + round, preRef, snapshot);
            lastGcTs = gcTs;
        }

        long[] preRefFinal = rgVisibility.getVisibilityBitmap(ts + 100);
        long[] finalSnap = rgVisibility.garbageCollect(ts + 100);
        assertBitmapsEqual("randomized final", preRefFinal, finalSnap);
    }

    // =====================================================================
    // exportChainItemsAfter / importDeletionChain JNI tests
    // =====================================================================

    @Test
    public void testExportChainItemsAfter()
    {
        rgVisibility.deleteRecord(5, 50);
        rgVisibility.deleteRecord(10, 100);
        rgVisibility.deleteRecord(15, 150);
        rgVisibility.deleteRecord(20, 200);
        rgVisibility.deleteRecord(300, 250);

        long[] items = rgVisibility.exportChainItemsAfter(100);

        assertNotNull("export should return non-null", items);
        assertEquals("interleaved pairs: 3 items × 2", 6, items.length);

        Set<Long> exportedTs = new HashSet<>();
        for (int i = 0; i < items.length; i += 2)
        {
            exportedTs.add(items[i + 1]);
        }
        assertTrue("should contain ts=150", exportedTs.contains(150L));
        assertTrue("should contain ts=200", exportedTs.contains(200L));
        assertTrue("should contain ts=250", exportedTs.contains(250L));
        assertFalse("should NOT contain ts=50", exportedTs.contains(50L));
        assertFalse("should NOT contain ts=100", exportedTs.contains(100L));
    }

    @Test
    public void testImportDeletionChain()
    {
        long safeGcTs = 100;
        try (RGVisibility newVis = new RGVisibility(ROW_COUNT, safeGcTs, null))
        {
            long[] items = {5, 150, 10, 200, 300, 250};
            newVis.importDeletionChain(items);

            long[] bitmap = newVis.getVisibilityBitmap(300);
            assertTrue("row 5 should be deleted",
                    (bitmap[5 / 64] & (1L << (5 % 64))) != 0);
            assertTrue("row 10 should be deleted",
                    (bitmap[10 / 64] & (1L << (10 % 64))) != 0);
            assertTrue("row 300 should be deleted",
                    (bitmap[300 / 64] & (1L << (300 % 64))) != 0);

            long[] partialBitmap = newVis.getVisibilityBitmap(180);
            assertTrue("row 5 at ts=180 should be deleted",
                    (partialBitmap[5 / 64] & (1L << (5 % 64))) != 0);
            assertFalse("row 10 at ts=180 should NOT be deleted",
                    (partialBitmap[10 / 64] & (1L << (10 % 64))) != 0);
        }
    }

    @Test
    public void testExportImportRoundTrip()
    {
        long safeGcTs = 100;

        rgVisibility.deleteRecord(5, 50);
        rgVisibility.deleteRecord(10, 80);
        rgVisibility.deleteRecord(15, 150);
        rgVisibility.deleteRecord(20, 200);
        rgVisibility.deleteRecord(300, 250);

        long[] exported = rgVisibility.exportChainItemsAfter(safeGcTs);
        assertEquals("3 items exported (ts=150,200,250)", 6, exported.length);

        try (RGVisibility newVis = new RGVisibility(ROW_COUNT, safeGcTs, null))
        {
            newVis.importDeletionChain(exported);

            for (long snapTs : new long[]{150, 200, 250, 500})
            {
                long[] oldBitmap = rgVisibility.getVisibilityBitmap(snapTs);
                long[] newBitmap = newVis.getVisibilityBitmap(snapTs);

                for (int row : new int[]{15, 20, 300})
                {
                    boolean oldDel = (oldBitmap[row / 64] & (1L << (row % 64))) != 0;
                    boolean newDel = (newBitmap[row / 64] & (1L << (row % 64))) != 0;
                    assertEquals("snap_ts=" + snapTs + " row=" + row, oldDel, newDel);
                }

                for (int row : new int[]{5, 10})
                {
                    boolean newDel = (newBitmap[row / 64] & (1L << (row % 64))) != 0;
                    assertFalse("row " + row + " (ts<=safeGcTs) should NOT be in new at snap=" + snapTs,
                            newDel);
                }
            }
        }
    }
}