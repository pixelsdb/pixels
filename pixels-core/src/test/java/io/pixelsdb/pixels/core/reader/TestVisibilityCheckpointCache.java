/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test client-side checkpoint loading and caching performance.
 */
public class TestVisibilityCheckpointCache
{
    private String testCheckpointDir;
    private Storage storage;
    private final long fileId = 999999L;
    private final int rgId = 0;

    @Before
    public void setUp() throws IOException
    {
        testCheckpointDir = ConfigFactory.Instance().getProperty("pixels.retina.checkpoint.dir");
        storage = StorageFactory.Instance().getStorage(testCheckpointDir);

        if (!storage.exists(testCheckpointDir))
        {
            storage.mkdirs(testCheckpointDir);
        }
    }

    private String resolve(String dir, String filename) {
        return dir.endsWith("/") ? dir + filename : dir + "/" + filename;
    }

    /**
     * Helper to create a dummy checkpoint file.
     */
    private void createDummyCheckpoint(String path, int numFiles, int rgsPerFile, long[] bitmap) throws IOException
    {
        try (DataOutputStream out = storage.create(path, true, 8 * 1024 * 1024))
        {
            out.writeInt(numFiles * rgsPerFile);
            for (int f = 0; f < numFiles; f++)
            {
                for (int r = 0; r < rgsPerFile; r++)
                {
                    out.writeLong((long)f);
                    out.writeInt(r);
                    out.writeInt(bitmap.length);
                    for (long l : bitmap)
                    {
                        out.writeLong(l);
                    }
                }
            }
            out.flush();
        }
    }

    @Test
    public void testCacheLoading() throws IOException
    {
        long timestamp = 1000L;
        String checkpointPath = resolve(testCheckpointDir, "vis_gc_tencent_100.bin");
        long[] dummyBitmap = new long[]{0x1L, 0x2L};
        createDummyCheckpoint(checkpointPath, 1, 1, dummyBitmap);

        VisibilityCheckpointCache cache = VisibilityCheckpointCache.getInstance();
        long[] loaded = cache.getVisibilityBitmap(timestamp, checkpointPath, 0L, 0);
        
        assertNotNull(loaded);
        assertTrue(loaded.length == 2);
        assertTrue(loaded[0] == 0x1L);
        assertTrue(loaded[1] == 0x2L);
    }

    /**
     * Migrated and adapted performance test.
     */
    @Test
    public void testCheckpointPerformance() throws InterruptedException, IOException
    {
        // 1. Configuration parameters
        int numFiles = 5000;
        int rgsPerFile = 1;
        int rowsPerRG = 200000; 
        int queryCount = 200;
        long timestamp = 2000L;
        String checkpointPath = resolve(testCheckpointDir, "perf_test_2000.bin");

        // 2. Prepare data
        int bitmapLen = (rowsPerRG + 63) / 64;
        long[] dummyBitmap = new long[bitmapLen];
        for(int i=0; i<bitmapLen; i++) dummyBitmap[i] = i;

        System.out.println("\n--- Starting Client-side Checkpoint Performance Test ---");
        createDummyCheckpoint(checkpointPath, numFiles, rgsPerFile, dummyBitmap);
        long fileSize = storage.getStatus(checkpointPath).getLength();

        // 3. Test Load performance
        VisibilityCheckpointCache cache = VisibilityCheckpointCache.getInstance();
        
        long firstLoadTimeNs = 0;
        long subsequentLoadsTotalNs = 0;
        java.util.Random randomForQuery = new java.util.Random();

        for (int i = 0; i < queryCount; i++)
        {
            int f = randomForQuery.nextInt(numFiles);
            int r = randomForQuery.nextInt(rgsPerFile);

            long start = System.nanoTime();
            cache.getVisibilityBitmap(timestamp, checkpointPath, (long)f, r);
            long end = System.nanoTime();

            if (i == 0)
            {
                firstLoadTimeNs = (end - start);
                System.out.printf("Cold Start Load (Triggered full file read): %.2f ms\n", firstLoadTimeNs / 1e6);
            }
            else
            {
                subsequentLoadsTotalNs += (end - start);
            }
        }

        double firstLoadTimeMs = firstLoadTimeNs / 1e6;
        double avgSubsequentLoadTimeMs = (subsequentLoadsTotalNs / (queryCount - 1.0)) / 1e6;
        double readThroughputMBs = (fileSize / (1024.0 * 1024.0)) / (firstLoadTimeMs / 1000.0);

        System.out.println("------------------------------------------------------------");
        System.out.printf("Checkpoint File Size:       %.2f MB\n", fileSize / (1024.0 * 1024.0));
        System.out.printf("First Load Time (Cold):     %.2f ms\n", firstLoadTimeMs);
        System.out.printf("Read/Parse Throughput:      %.2f MB/s\n", readThroughputMBs);
        System.out.printf("Avg Memory Hit Latency:     %.6f ms\n", avgSubsequentLoadTimeMs);
        System.out.printf("Total Time for %d queries:  %.2f ms\n", queryCount, (firstLoadTimeNs + subsequentLoadsTotalNs) / 1e6);
        System.out.println("------------------------------------------------------------");
    }
}
