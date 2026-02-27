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
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test checkpoint creation and recovery logic in Retina side.
 */
public class TestRetinaCheckpoint
{
    private RetinaResourceManager retinaManager;
    private String testCheckpointDir;
    private Storage storage;
    private final long fileId = 999999L;
    private final int rgId = 0;
    private final int numRows = 1024;
    private String hostName;

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
        hostName = System.getenv("HOSTNAME");
        if (hostName == null)
        {
            hostName = InetAddress.getLocalHost().getHostName();
        }
    }

    private String resolve(String dir, String filename) {
        return dir.endsWith("/") ? dir + filename : dir + "/" + filename;
    }

    private String getOffloadFileName(long timestamp) {
        return RetinaUtils.getCheckpointFileName(RetinaUtils.CHECKPOINT_PREFIX_OFFLOAD, hostName, timestamp);
    }

    private String getGcFileName(long timestamp) {
        return RetinaUtils.getCheckpointFileName(RetinaUtils.CHECKPOINT_PREFIX_GC, hostName, timestamp);
    }

    @Test
    public void testRegisterOffload() throws RetinaException, IOException
    {
        System.out.println("\n[Test] Starting testRegisterOffload...");
        retinaManager.addVisibility(fileId, rgId, numRows);
        long timestamp = 100L;

        // Register offload
        System.out.println("Registering offload for timestamp: " + timestamp);
        retinaManager.registerOffload(timestamp);

        // Verify checkpoint file exists
        String expectedFile = resolve(testCheckpointDir, getOffloadFileName(timestamp));
        assertTrue("Offload checkpoint file should exist", storage.exists(expectedFile));
        System.out.println("Verified: Checkpoint file exists at " + expectedFile);

        // Unregister
        System.out.println("Unregistering offload...");
        retinaManager.unregisterOffload(timestamp);

        // File should be removed
        assertFalse("Offload checkpoint file should be removed", storage.exists(expectedFile));
        System.out.println("Verified: Checkpoint file removed. testRegisterOffload passed.");
    }

    @Test
    public void testMultipleOffloads() throws RetinaException, IOException
    {
        System.out.println("\n[Test] Starting testMultipleOffloads...");
        retinaManager.addVisibility(fileId, rgId, numRows);
        long timestamp1 = 100L;
        long timestamp1_dup = 100L; // same timestamp

        // Both register the same timestamp - should share checkpoint
        System.out.println("Registering same timestamp twice...");
        retinaManager.registerOffload(timestamp1);
        retinaManager.registerOffload(timestamp1_dup);

        String expectedFile = resolve(testCheckpointDir, getOffloadFileName(timestamp1));
        assertTrue("Offload checkpoint file should exist", storage.exists(expectedFile));

        // Unregister one - should not remove yet (ref count >1)
        System.out.println("Unregistering once (ref count should still be > 0)...");
        retinaManager.unregisterOffload(timestamp1);
        assertTrue("Offload checkpoint should still exist (ref count >1)", storage.exists(expectedFile));
        System.out.println("Verified: Checkpoint still exists after one unregister.");

        // Unregister second
        System.out.println("Unregistering second time...");
        retinaManager.unregisterOffload(timestamp1);
        assertFalse("Offload checkpoint should be removed", storage.exists(expectedFile));
        System.out.println("Verified: Checkpoint removed after final unregister. testMultipleOffloads passed.");
    }

    @Test
    public void testCheckpointRecovery() throws RetinaException, IOException
    {
        System.out.println("\n[Test] Starting testCheckpointRecovery...");
        retinaManager.addVisibility(fileId, rgId, numRows);
        long timestamp = 100L;

        // 1. Delete row 10
        int rowToDelete = 10;
        System.out.println("Deleting row " + rowToDelete + " in memory...");
        retinaManager.deleteRecord(fileId, rgId, rowToDelete, timestamp);

        // Verify deleted in memory
        long[] memBitmap = retinaManager.queryVisibility(fileId, rgId, timestamp);
        assertTrue("Row 10 should be deleted in memory", isBitSet(memBitmap, rowToDelete));

        // 2. Register Offload to generate checkpoint file
        System.out.println("Creating checkpoint on disk...");
        retinaManager.registerOffload(timestamp);
        String offloadPath = resolve(testCheckpointDir, getOffloadFileName(timestamp));
        assertTrue("Checkpoint file should exist", storage.exists(offloadPath));

        // 3. Rename offload file to GC file to simulate checkpoint generated by GC
        String gcPath = resolve(testCheckpointDir, getGcFileName(timestamp));
        System.out.println("Simulating GC checkpoint by renaming offload file to: " + gcPath);
        // Storage interface doesn't have rename, using copy and delete
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
        System.out.println("Simulating system restart (resetting memory state)...");
        resetSingletonState();

        // 5. Perform recovery
        System.out.println("Running recoverCheckpoints()...");
        // At this point rgVisibilityMap is empty, recoverCheckpoints will load data directly into rgVisibilityMap
        retinaManager.recoverCheckpoints();

        // 6. Verify recovered state immediately after recovery
        System.out.println("Verifying recovered state immediately after recoverCheckpoints()...");
        long[] recoveredBitmap = retinaManager.queryVisibility(fileId, rgId, timestamp);
        assertTrue("Row 10 should be deleted after recovery", isBitSet(recoveredBitmap, rowToDelete));
        assertFalse("Row 11 should not be deleted", isBitSet(recoveredBitmap, rowToDelete + 1));

        // 7. Re-add Visibility, at this point it should see that it already exists in rgVisibilityMap
        System.out.println("Re-adding visibility for file (should skip as it already exists)...");
        retinaManager.addVisibility(fileId, rgId, numRows);

        // 8. Verify state still correct
        long[] finalBitmap = retinaManager.queryVisibility(fileId, rgId, timestamp);
        assertTrue("Row 10 should still be deleted", isBitSet(finalBitmap, rowToDelete));
        System.out.println("Verified: Recovery successful, row state restored directly to map. testCheckpointRecovery passed.");
    }

    @Test
    public void testCheckpointRetryAfterFailure() throws RetinaException, IOException
    {
        System.out.println("\n[Test] Starting testCheckpointRetryAfterFailure...");
        retinaManager.addVisibility(fileId, rgId, numRows);
        long timestamp = 123L;

        String expectedFile = resolve(testCheckpointDir, getOffloadFileName(timestamp));

        // 1. Pre-create a DIRECTORY with the same name to cause creation failure
        storage.mkdirs(expectedFile);
        System.out.println("Created a directory at " + expectedFile + " to simulate failure.");

        // 2. Try to register offload - should fail
        try
        {
            retinaManager.registerOffload(timestamp);
            assertTrue("Should have thrown an exception", false);
        } catch (RetinaException e)
        {
            System.out.println("Expected failure occurred: " + e.getMessage());
        }

        // 3. Remove the directory
        storage.delete(expectedFile, true);
        assertFalse("Directory should be removed", storage.exists(expectedFile));

        // 4. Try again - should succeed now because we clear failed futures
        System.out.println("Retrying registration...");
        retinaManager.registerOffload(timestamp);

        assertTrue("Offload checkpoint file should exist after retry", storage.exists(expectedFile));
        System.out.println("Verified: Retry successful. testCheckpointRetryAfterFailure passed.");
    }

    @Test
    public void testMultiRGCheckpoint() throws RetinaException, IOException
    {
        System.out.println("\n[Test] Starting testMultiRGCheckpoint...");
        int numRgs = 3;
        for (int i = 0; i < numRgs; i++)
        {
            retinaManager.addVisibility(fileId, i, numRows);
        }
        long timestamp = 200L;

        // Delete records in different RGs
        retinaManager.deleteRecord(fileId, 0, 10, timestamp);
        retinaManager.deleteRecord(fileId, 1, 20, timestamp);
        retinaManager.deleteRecord(fileId, 2, 30, timestamp);

        // Create checkpoint
        retinaManager.registerOffload(timestamp);
        String offloadPath = resolve(testCheckpointDir, getOffloadFileName(timestamp));
        
        // Simulating GC checkpoint for recovery
        String gcPath = resolve(testCheckpointDir, getGcFileName(timestamp));
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

        // Reset and recover
        resetSingletonState();
        retinaManager.recoverCheckpoints();

        // Verify all RGs
        assertTrue("RG 0 row 10 should be deleted", isBitSet(retinaManager.queryVisibility(fileId, 0, timestamp), 10));
        assertTrue("RG 1 row 20 should be deleted", isBitSet(retinaManager.queryVisibility(fileId, 1, timestamp), 20));
        assertTrue("RG 2 row 30 should be deleted", isBitSet(retinaManager.queryVisibility(fileId, 2, timestamp), 30));
        
        System.out.println("Verified: Multi-RG state correctly restored. testMultiRGCheckpoint passed.");
    }

    @Test
    public void testCheckpointDataIntegrity() throws RetinaException, IOException
    {
        System.out.println("\n[Test] Starting testCheckpointDataIntegrity...");
        int numRgs = 5;
        for (int i = 0; i < numRgs; i++)
        {
            retinaManager.addVisibility(fileId, i, numRows);
        }
        long timestamp = 300L;

        retinaManager.registerOffload(timestamp);
        String path = resolve(testCheckpointDir, getOffloadFileName(timestamp));

        // Directly read file to verify header
        try (DataInputStream in = storage.open(path))
        {
            int savedRgs = in.readInt();
            assertTrue("Saved RG count " + savedRgs + " should match " + numRgs, savedRgs == numRgs);
        }
        System.out.println("Verified: Data integrity (header) is correct. testCheckpointDataIntegrity passed.");
    }

    @Test
    public void testConcurrency() throws InterruptedException, RetinaException
    {
        System.out.println("\n[Test] Starting testConcurrency with 20 threads...");
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
                            // Query visibility from memory
                            long[] bitmap = retinaManager.queryVisibility(fileId, rgId, timestamp);
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

            Field refCountsField = RetinaResourceManager.class.getDeclaredField("checkpointRefCounts");
            refCountsField.setAccessible(true);
            ((Map<?, ?>) refCountsField.get(retinaManager)).clear();

            Field gcTimestampField = RetinaResourceManager.class.getDeclaredField("latestGcTimestamp");
            gcTimestampField.setAccessible(true);
            gcTimestampField.setLong(retinaManager, -1L);

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
}
