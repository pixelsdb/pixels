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
package io.pixelsdb.pixels.worker.common;

import org.junit.Test;
import java.util.concurrent.*;
import static org.junit.Assert.*;

public class TestPartitionPipeline
{
    @Test(timeout = 5000)
    public void differentServiceCostsChangeConcurrencyAndParkBeforeReading() throws Exception
    {
        PartitionPipeline pipeline = new PartitionPipeline(6, 4);
        long bytes = 1L << 20;
        // Slow input, fast output must be allowed more reader concurrency.
        for (int i = 0; i < 40; i++) { pipeline.scanned(bytes, 100000000); pipeline.written(bytes, 1000000); }
        assertEquals(Long.valueOf(6), pipeline.snapshot().get("scanConcurrencyTarget"));
        // Reverse the bottleneck using the same algorithm, without another configuration.
        for (int i = 0; i < 160; i++) { pipeline.scanned(bytes, 1000000); pipeline.written(bytes, 100000000); }
        assertEquals(Long.valueOf(1), pipeline.snapshot().get("scanConcurrencyTarget"));
        pipeline.acquire(() -> {});
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try
        {
            Future<?> waiting = caller.submit(() -> { pipeline.acquire(() -> {}); pipeline.release(); });
            assertThrows(TimeoutException.class, () -> waiting.get(100, TimeUnit.MILLISECONDS));
            pipeline.release(); waiting.get(1, TimeUnit.SECONDS);
            assertTrue(pipeline.snapshot().get("scanSchedulingWaitNs") > 0);
        }
        finally { caller.shutdownNow(); assertTrue(caller.awaitTermination(1, TimeUnit.SECONDS)); }
    }

    @Test(timeout = 5000)
    public void cancellationCanStopAReaderWaitingForConcurrency() throws Exception
    {
        PartitionPipeline pipeline = new PartitionPipeline(6, 2);
        pipeline.acquire(() -> {});
        WorkerCancellation cancellation = new WorkerCancellation();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try
        {
            Future<?> waiting = caller.submit(() -> pipeline.acquire(cancellation::check));
            assertThrows(TimeoutException.class, () -> waiting.get(100, TimeUnit.MILLISECONDS));
            cancellation.cancel(new RuntimeException("cancel paced reader"));
            assertThrows(ExecutionException.class, () -> waiting.get(1, TimeUnit.SECONDS));
        }
        finally { pipeline.release(); caller.shutdownNow(); assertTrue(caller.awaitTermination(1, TimeUnit.SECONDS)); }
    }
}
