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
import org.junit.Before;
import org.junit.Test;
import com.google.common.util.concurrent.RateLimiter;

import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class TestFlushObject
{
    private static final RetinaResourceManager retinaResourceManager = RetinaResourceManager.Instance();
    private static final String schemaName = "tpch";
    private static final String tableName_1 = "nation";
    private static final String tableName_2 = "region";
    private static final AtomicLong table_1_key = new AtomicLong(0);
    private static final AtomicLong table_2_key = new AtomicLong(0);

    private static final double INSERTS_PER_SECOND = 200.0;
    private static final int  TOTAL_RECORDS_TO_INSERT = 10000;

    private final RateLimiter nationRateLimiter = RateLimiter.create(INSERTS_PER_SECOND);
    private final RateLimiter regionRateLimiter = RateLimiter.create(INSERTS_PER_SECOND);

    @Before
    public void setup()
    {
        try
        {
            retinaResourceManager.addWriteBuffer(schemaName, tableName_1);
            retinaResourceManager.addWriteBuffer(schemaName, tableName_2);
        } catch (RetinaException e)
        {
            throw new RuntimeException("Failed to setup RetinaResourceManager", e);
        }
    }

    private byte[][] createNationRow()
    {
        long key = table_1_key.getAndIncrement();
        byte[][] row  = new byte[4][];
        row[0] = ByteBuffer.allocate(8).putLong(key).array();  // bigint
        row[1] = ("NATION_" + key).getBytes();                   // char(25)
        row[2] = ByteBuffer.allocate(8).putLong(key).array();  // bigint
        row[3] = ("This is nation " + key).getBytes();           // varchar(152)
        return row;
    }

    private byte[][] createRegionRow()
    {
        long key = table_2_key.getAndIncrement();
        byte[][] row  = new byte[3][];
        row[0] = ByteBuffer.allocate(8).putLong(key).array();  // bigint
        row[1] = ("REGION_" + key).getBytes();                   // char(25)
        row[2] = ("This is region " + key).getBytes();           // varchar(152)
        return row;
    }

    private class RecordInserter implements Runnable
    {
        private final String tableName;
        private final RateLimiter rateLimiterr;
        private final Supplier<byte[][]> rowSupplier;

        public RecordInserter(String tableName, RateLimiter rateLimiter, Supplier<byte[][]> rowSupplier)
        {
            this.tableName = tableName;
            this.rateLimiterr = rateLimiter;
            this.rowSupplier = rowSupplier;
        }

        @Override
        public void run()
        {
            System.out.println("Starting insertion thread for table: " + tableName +
                    " with rate limit: " + INSERTS_PER_SECOND + " op/sec.");

            for (int i = 0; i < TOTAL_RECORDS_TO_INSERT; ++i)
            {
                rateLimiterr.acquire();
                try
                {
                    byte[][] row = rowSupplier.get();
                    retinaResourceManager.insertRecord(schemaName, tableName, row, 0, 0);
                } catch (RetinaException e)
                {
                    System.out.println("Error inserting record into table " + tableName + ": " + e.getMessage());
                    break;
                }
            }
            System.out.println("Finished insertion thread for table: " + tableName +
                    ". Total records inserted: " + TOTAL_RECORDS_TO_INSERT);
        }
    }

    @Test
    public void testSerialization() throws InterruptedException
    {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        Future<?> future1 = executor.submit(new RecordInserter(
                tableName_1, nationRateLimiter, this::createNationRow));
        Future<?> future2 = executor.submit(new RecordInserter(
                tableName_2, regionRateLimiter, this::createRegionRow));

        try
        {
            future1.get();
            future2.get();
        } catch (Exception e)
        {
            System.out.println("Error during record insertion: " + e.getMessage());
            future1.cancel(true);
            future2.cancel(true);
        } finally
        {
            executor.shutdown();
            if (!executor.awaitTermination(20, TimeUnit.SECONDS))
            {
                System.err.println("Threads did not terminate in time. Forcing shutdown.");
                executor.shutdownNow();
            }
            Thread.sleep(30000); // sleep 30 seconds to ensure s3 put/write are done
            System.out.println("Record insertion test completed for both tables.");
        }
    }
}

