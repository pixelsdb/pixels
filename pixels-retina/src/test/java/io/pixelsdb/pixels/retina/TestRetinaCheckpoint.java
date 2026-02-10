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

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test checkpoint creation and recovery logic.
 */
public class TestRetinaCheckpoint
{
    private RetinaResourceManager retinaManager;
    private String testCheckpointDir;
    private Storage storage;
    private final long fileId = 999999L;
    private final int rgId = 0;
    private final int numRows = 1024;

    @Before
    public void setUp() throws IOException, RetinaException
    {
        testCheckpointDir = ConfigFactory.Instance().getProperty("pixels.retina.checkpoint.dir");
        storage = StorageFactory.Instance().getStorage(testCheckpointDir);

        if (!storage.exists(testCheckpointDir))
        {
            storage.mkdirs(testCheckpointDir);
        } else
        {
            for (String path : storage.listPaths(testCheckpointDir))
            {
                try
                {
                    storage.delete(path, false);
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }

        retinaManager = RetinaResourceManager.Instance();
        resetSingletonState();
    }

    private String resolve(String dir, String filename) {
        return dir.endsWith("/") ? dir + filename : dir + "/" + filename;
    }

    @Test
    public void testRegisterOffload() throws RetinaException, IOException
    {
        retinaManager.addVisibility(fileId, rgId, numRows);
        long timestamp = 100L;

        // Register offload
        retinaManager.registerOffload(timestamp);

        // Verify checkpoint file exists
        String expectedFile = resolve(testCheckpointDir, "vis_offload_100.bin");
        assertTrue("Offload checkpoint file should exist", storage.exists(expectedFile));

        // Unregister
        retinaManager.unregisterOffload(timestamp);

        // File should be removed
        assertFalse("Offload checkpoint file should be removed", storage.exists(expectedFile));
    }

    @Test
    public void testMultipleOffloads() throws RetinaException, IOException
    {
        retinaManager.addVisibility(fileId, rgId, numRows);
        long timestamp1 = 100L;
        long timestamp1_dup = 100L; // same timestamp

        // Both register the same timestamp - should share checkpoint
        retinaManager.registerOffload(timestamp1);
        retinaManager.registerOffload(timestamp1_dup);

        String expectedFile = resolve(testCheckpointDir, "vis_offload_100.bin");
        assertTrue("Offload checkpoint file should exist", storage.exists(expectedFile));

        // Unregister one - should not remove yet (ref count >1)
        retinaManager.unregisterOffload(timestamp1);
        assertTrue("Offload checkpoint should still exist (ref count >1)", storage.exists(expectedFile));

        // Unregister second
        retinaManager.unregisterOffload(timestamp1);
        assertFalse("Offload checkpoint should be removed", storage.exists(expectedFile));
    }

    @Test
    public void testCheckpointRecovery() throws RetinaException, IOException
    {
        retinaManager.addVisibility(fileId, rgId, numRows);
        long timestamp = 100L;

        // 1. Delete row 10
        int rowToDelete = 10;
        retinaManager.deleteRecord(fileId, rgId, rowToDelete, timestamp);

        // Verify deleted in memory
        long[] memBitmap = retinaManager.queryVisibility(fileId, rgId, timestamp);
        assertTrue("Row 10 should be deleted in memory", isBitSet(memBitmap, rowToDelete));

        // 2. Register Offload to generate checkpoint file
        retinaManager.registerOffload(timestamp);
        String offloadPath = resolve(testCheckpointDir, "vis_offload_" + timestamp + ".bin");
        assertTrue("Checkpoint file should exist", storage.exists(offloadPath));

        // 3. Rename offload file to GC file to simulate checkpoint generated by GC
        String gcPath = resolve(testCheckpointDir, "vis_gc_" + timestamp + ".bin");
        // Storage interface doesn't have renamed, using copy and delete
        try (DataInputStream in = storage.open(offloadPath);
             DataOutputStream out = storage.create(gcPath, true, 4096))
        {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1)
            {
                out.write(buffer, 0, bytesRead);
            }
        }
        storage.delete(offloadPath, false);

        // 4. Reset singleton state (Simulate Crash/Restart)
        resetSingletonState();

        // 5. Perform recovery
        // At this point rgVisibilityMap is empty, need to call recoverCheckpoints to load data into cache
        retinaManager.recoverCheckpoints();

        // 6. Re-add Visibility, at this point it should recover state from recoveryCache instead of creating new
        retinaManager.addVisibility(fileId, rgId, numRows);

        // 7. Verify recovered state: Row 10 should still be in deleted state
        long[] recoveredBitmap = retinaManager.queryVisibility(fileId, rgId, timestamp);
        assertTrue("Row 10 should still be deleted after recovery", isBitSet(recoveredBitmap, rowToDelete));
        assertFalse("Row 11 should not be deleted", isBitSet(recoveredBitmap, rowToDelete + 1));
    }

    @Test
    public void testDiskBitmapQuery() throws RetinaException
    {
        retinaManager.addVisibility(fileId, rgId, numRows);
        long baseTimestamp = 200L;
        long transId = 888L;

        // 1. Delete row 5 at baseTimestamp
        retinaManager.deleteRecord(fileId, rgId, 5, baseTimestamp);

        // 2. Register Offload for this transaction (save snapshot at this moment to disk)
        retinaManager.registerOffload(baseTimestamp);

        // 3. Delete row 6 at a later time baseTimestamp + 10
        // This only affects the latest state in memory, should not affect the checkpoint on disk
        retinaManager.deleteRecord(fileId, rgId, 6, baseTimestamp + 10);

        // 4. Case A: Query using transId (should read disk Checkpoint)
        // Expected: Row 5 deleted, Row 6 not deleted (deleted after checkpoint)
        long[] diskBitmap = retinaManager.queryVisibility(fileId, rgId, baseTimestamp, transId);
        assertTrue("Disk: Row 5 should be deleted", isBitSet(diskBitmap, 5));
        assertFalse("Disk: Row 6 should NOT be deleted (deleted after checkpoint)", isBitSet(diskBitmap, 6));

        // 5. Case B: Query without transId (read memory)
        // Expected: Query at a later timestamp, both rows 5 and 6 are deleted
        long[] memBitmap = retinaManager.queryVisibility(fileId, rgId, baseTimestamp + 20);
        assertTrue("Memory: Row 5 should be deleted", isBitSet(memBitmap, 5));
        assertTrue("Memory: Row 6 should be deleted", isBitSet(memBitmap, 6));

        // Cleanup
        retinaManager.unregisterOffload(baseTimestamp);
    }

    @Test
    public void testConcurrency() throws InterruptedException, RetinaException
    {
        retinaManager.addVisibility(fileId, rgId, numRows);
        int numThreads = 20;
        int operationsPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicBoolean errorOccurred = new AtomicBoolean(false);

        // Pre-delete a row to ensure base state
        retinaManager.deleteRecord(fileId, rgId, 0, 10L);

        for (int i = 0; i < numThreads; i++)
        {
            final int threadId = i;
            executor.submit(() -> {
                try
                {
                    startLatch.await();
                    long transId = 10000L + threadId;
                    long timestamp = 500L + (threadId % 5) * 10; // Multiple threads may share the same timestamp

                    for (int j = 0; j < operationsPerThread; j++)
                    {
                        // Random operation mix
                        if (j % 3 == 0)
                        {
                            // Register Offload
                            retinaManager.registerOffload(timestamp);
                        }
                        else if (j % 3 == 1)
                        {
                            // Query visibility
                            long[] bitmap = retinaManager.queryVisibility(fileId, rgId, timestamp, transId);
                            if (!isBitSet(bitmap, 0)) {
                                throw new RuntimeException("Row 0 should be deleted in all views");
                            }
                        }
                        else
                        {
                            // Unregister Offload
                            retinaManager.unregisterOffload(timestamp);
                        }
                    }
                } catch (Exception e)
                {
                    e.printStackTrace();
                    errorOccurred.set(true);
                } finally
                {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // Start all threads
        boolean finished = doneLatch.await(60, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertTrue("Timeout waiting for concurrency test", finished);
        assertFalse("Errors occurred during concurrency test", errorOccurred.get());
    }

    /**
     * Use reflection to reset internal state of RetinaResourceManager, simulating a restart.
     */
    private void resetSingletonState()
    {
        try
        {
            Field rgMapField = RetinaResourceManager.class.getDeclaredField("rgVisibilityMap");
            rgMapField.setAccessible(true);
            ((Map<?, ?>) rgMapField.get(retinaManager)).clear();

            Field bufferMapField = RetinaResourceManager.class.getDeclaredField("pixelsWriteBufferMap");
            bufferMapField.setAccessible(true);
            ((Map<?, ?>) bufferMapField.get(retinaManager)).clear();

            Field offloadedField = RetinaResourceManager.class.getDeclaredField("offloadedCheckpoints");
            offloadedField.setAccessible(true);
            ((Map<?, ?>) offloadedField.get(retinaManager)).clear();

            Field offloadCacheField = RetinaResourceManager.class.getDeclaredField("offloadCache");
            offloadCacheField.setAccessible(true);
            ((Map<?, ?>) offloadCacheField.get(retinaManager)).clear();

            Field refCountsField = RetinaResourceManager.class.getDeclaredField("checkpointRefCounts");
            refCountsField.setAccessible(true);
            ((Map<?, ?>) refCountsField.get(retinaManager)).clear();

            Field gcTimestampField = RetinaResourceManager.class.getDeclaredField("latestGcTimestamp");
            gcTimestampField.setAccessible(true);
            gcTimestampField.setLong(retinaManager, -1L);

            Field recoveryCacheField = RetinaResourceManager.class.getDeclaredField("recoveryCache");
            recoveryCacheField.setAccessible(true);
            ((Map<?, ?>) recoveryCacheField.get(retinaManager)).clear();

        } catch (Exception e)
        {
            throw new RuntimeException("Failed to reset singleton state", e);
        }
    }

    private boolean isBitSet(long[] bitmap, int rowIndex)
    {
        if (bitmap == null || bitmap.length == 0) return false;

        int longIndex = rowIndex / 64;
        int bitOffset = rowIndex % 64;

        if (longIndex >= bitmap.length) return false;

        return (bitmap[longIndex] & (1L << bitOffset)) != 0;
    }

    /**
     * Test the performance and memory overhead of checkpoint offload and load.
     * <p>
     * Run Command:
     * LD_PRELOAD=/lib/x86_64-linux-gnu/libjemalloc.so.2 mvn test \
     *   -Dtest=TestRetinaCheckpoint#testCheckpointPerformance \
     *   -pl pixels-retina \
     *   -DargLine="-Xms40g -Xmx40g"
     */
    @Test
    public void testCheckpointPerformance() throws RetinaException, InterruptedException, IOException
    {
        // 1. Configuration parameters
        int numFiles = 500;
        int rgsPerFile = 1;
        int rowsPerRG = 200000; // rows per Row Group
        long totalRecords = (long) numFiles * rgsPerFile * rowsPerRG;
        double targetDeleteRatio = 0.1;
        int queryCount = 200;

        long timestamp = 1000L;
        long transId = 2000L;

        System.out.println("\n============================================================");
        System.out.println("--- Starting Checkpoint Performance Test ---");
        System.out.printf("Config: %d files, %d RGs/file, %d rows/RG, %d queries\n",
                numFiles, rgsPerFile, rowsPerRG, queryCount);
        System.out.printf("Target Delete Ratio: %.2f%%\n", targetDeleteRatio * 100);
        System.out.println("============================================================\n");

        // 2. Initialize data and perform random deletes
        LongAdder totalActualDeletedRows = new LongAdder();

        // Step A: Pre-add Visibility
        for (int f = 0; f < numFiles; f++) {
            for (int r = 0; r < rgsPerFile; r++) {
                retinaManager.addVisibility(f, r, rowsPerRG);
            }
        }

        // Step B: Parallel deleteRecord
        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        CountDownLatch latch = new CountDownLatch(numFiles * rgsPerFile);
        for (int f = 0; f < numFiles; f++)
        {
            for (int r = 0; r < rgsPerFile; r++)
            {
                final int fileId = f;
                final int rgId = r;
                executor.submit(() -> {
                    try
                    {
                        java.util.Random randomInThread = new java.util.Random();
                        int targetDeleteCount = (int) (rowsPerRG * targetDeleteRatio);

                        int actualDeletedCount = 0;
                        java.util.Set<Integer> deletedInRG = new java.util.HashSet<>();
                        while (deletedInRG.size() < targetDeleteCount)
                        {
                            int rowId = randomInThread.nextInt(rowsPerRG);
                            if (deletedInRG.add(rowId))
                            {
                                retinaManager.deleteRecord(fileId, rgId, rowId, timestamp);
                                actualDeletedCount++;
                            }
                        }
                        totalActualDeletedRows.add(actualDeletedCount);
                    } catch (Exception e)
                    {
                        e.printStackTrace();
                    } finally
                    {
                        latch.countDown();
                    }
                });
            }
        }
        latch.await(30, TimeUnit.MINUTES);
        executor.shutdown();

        double actualDeleteRatio = (double) totalActualDeletedRows.sum() / totalRecords;
        System.out.printf("[Data Gen] Total Deleted Rows: %d (Actual Ratio: %.4f%%)\n",
                totalActualDeletedRows.sum(), actualDeleteRatio * 100);

        // 3. Test Offload (Checkpoint Creation) performance
        System.out.println("\n[Phase 1] Testing Checkpoint Offload (Write)...");
        long startOffload = System.nanoTime();
        retinaManager.registerOffload(timestamp);
        long endOffload = System.nanoTime();

        // [Accuracy] Calculate offload peak memory overhead AFTER timing to avoid interference.
        // We simulate the snapshot logic to get the exact physical size of long arrays being offloaded.
        long offloadPeakBytes = calculateOffloadPeakMemory(timestamp);

        double offloadTimeMs = (endOffload - startOffload) / 1e6;
        String offloadPath = resolve(testCheckpointDir, "vis_offload_" + timestamp + ".bin");
        long fileSize = storage.getStatus(offloadPath).getLength();
        double writeThroughputMBs = (fileSize / (1024.0 * 1024.0)) / (offloadTimeMs / 1000.0);

        System.out.println("------------------------------------------------------------");
        System.out.printf("Total Offload Time:         %.2f ms\n", offloadTimeMs);
        System.out.printf("Checkpoint File Size:       %.2f MB\n", fileSize / (1024.0 * 1024.0));
        System.out.printf("Offload Peak Mem Overhead:  %.2f MB\n", offloadPeakBytes / (1024.0 * 1024.0));
        System.out.printf("Write Throughput:           %.2f MB/s\n", writeThroughputMBs);
        System.out.printf("Avg Offload Time per RG:    %.4f ms\n", offloadTimeMs / (numFiles * rgsPerFile));
        System.out.println("------------------------------------------------------------");

        // 4. Test Load (Checkpoint Load) performance
        System.out.println("\n[Phase 2] Testing Checkpoint Load (Read)...");
        long firstLoadTimeNs = 0;
        long subsequentLoadsTotalNs = 0;
        java.util.Random randomForQuery = new java.util.Random();

        for (int i = 0; i < queryCount; i++)
        {
            int f = randomForQuery.nextInt(numFiles);
            int r = randomForQuery.nextInt(rgsPerFile);

            long start = System.nanoTime();
            retinaManager.queryVisibility(f, r, timestamp, transId);
            long end = System.nanoTime();

            if (i == 0)
            {
                firstLoadTimeNs = (end - start);
                System.out.printf("Cold Start Query (Triggered full file load): %.2f ms\n", firstLoadTimeNs / 1e6);
            }
            else
            {
                subsequentLoadsTotalNs += (end - start);
            }
        }

        // [Accuracy] Calculate load memory overhead AFTER timing via reflection on offloadCache.
        long loadCacheBytes = calculateLoadCacheMemory(timestamp);

        double firstLoadTimeMs = firstLoadTimeNs / 1e6;
        double avgSubsequentLoadTimeMs = (subsequentLoadsTotalNs / (queryCount - 1.0)) / 1e6;
        double readThroughputMBs = (fileSize / (1024.0 * 1024.0)) / (firstLoadTimeMs / 1000.0);

        System.out.println("------------------------------------------------------------");
        System.out.printf("First Load Time (Cold):     %.2f ms\n", firstLoadTimeMs);
        System.out.printf("Load Memory Overhead:       %.2f MB\n", loadCacheBytes / (1024.0 * 1024.0));
        System.out.printf("Read/Parse Throughput:      %.2f MB/s\n", readThroughputMBs);
        System.out.printf("Avg Memory Hit Latency:     %.6f ms\n", avgSubsequentLoadTimeMs);
        System.out.printf("Total Time for %d queries:  %.2f ms\n", queryCount, (firstLoadTimeNs + subsequentLoadsTotalNs) / 1e6);
        System.out.println("------------------------------------------------------------");

        // 5. Cleanup
        retinaManager.unregisterOffload(timestamp);
        System.out.println("\n--- Checkpoint Performance Test Finished ---\n");
    }

    /**
     * Accurately calculate the memory size of long arrays that would be captured in a snapshot.
     * This represents the peak heap usage during the offload process.
     */
    private long calculateOffloadPeakMemory(long timestamp)
    {
        try {
            Field rgMapField = RetinaResourceManager.class.getDeclaredField("rgVisibilityMap");
            rgMapField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, RGVisibility> rgMap = (Map<String, RGVisibility>) rgMapField.get(retinaManager);
            
            long totalBytes = 0;
            for (RGVisibility visibility : rgMap.values()) {
                long[] bitmap = visibility.getVisibilityBitmap(timestamp);
                if (bitmap != null) {
                    totalBytes += (long) bitmap.length * 8;
                }
            }
            return totalBytes;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * Accurately calculate the memory size of long arrays currently stored in offloadCache.
     * This represents the persistent heap usage after loading a checkpoint.
     */
    private long calculateLoadCacheMemory(long timestamp)
    {
        try {
            Field offloadCacheField = RetinaResourceManager.class.getDeclaredField("offloadCache");
            offloadCacheField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<Long, Map<String, long[]>> offloadCache = (Map<Long, Map<String, long[]>>) offloadCacheField.get(retinaManager);
            
            Map<String, long[]> cacheForTs = offloadCache.get(timestamp);
            if (cacheForTs == null) return 0;
            
            long totalBytes = 0;
            for (long[] bitmap : cacheForTs.values()) {
                totalBytes += (long) bitmap.length * 8;
            }
            return totalBytes;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }
}
