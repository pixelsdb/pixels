/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.storage.s3qs.*;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestS3QSAsyncIO
{
    @Test(timeout = 10000)
    public void idleWriterStartsBeforeTheNormalGroupTargetAndRefillsWithoutScanning() throws Exception
    {
        Fixture f = new Fixture(16);
        CountDownLatch firstPut = new CountDownLatch(1), release = new CountDownLatch(1), threePuts = new CountDownLatch(3);
        AtomicInteger puts = new AtomicInteger();
        when(f.s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call -> {
            threePuts.countDown();
            if (puts.incrementAndGet() == 1)
            {
                firstPut.countDown(); assertTrue(release.await(3, TimeUnit.SECONDS));
            }
            return PutObjectResponse.builder().build();
        });
        PartitionByteBudget budget = new PartitionByteBudget(1024 * 1024);
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 16, f.context, 65536, 1, budget))
        {
            try
            {
                output.add(0, batch(16), f.schema);
                assertFalse("subminimum groups should keep aggregating", firstPut.await(100, TimeUnit.MILLISECONDS));
                output.add(0, batch(1024), f.schema);
                for (int i = 1; i < 16; i++) { output.add(i, batch(768), f.schema); }
                assertTrue("a partial group should start before finish", firstPut.await(2, TimeUnit.SECONDS));
                output.add(1, batch(1024), f.schema);
                output.add(2, batch(2048), f.schema);
                output.add(3, batch(16), f.schema);
                assertEquals("busy writers should let partial groups grow", 1, puts.get());
                release.countDown();
                assertTrue("completion should refill even with no new scan batches", threePuts.await(2, TimeUnit.SECONDS));
                output.finish(f.schema, new HashSet<>());
                List<Integer> order = new ArrayList<>();
                for (S3QueueMessage message : f.sent)
                {
                    if (message.isData()) { order.add(message.getPartitionId()); }
                }
                assertEquals(Arrays.asList(0, 2, 1), order.subList(0, 3));
                assertEquals(4128 + 15 * 768, f.sent.stream().filter(S3QueueMessage::isData).mapToLong(S3QueueMessage::getRowCount).sum());
                f.verifyEnds(16);
            }
            finally { release.countDown(); }
        }
        assertEquals(0, budget.used());
        assertTrue(f.context.getCancellation().isQuiescent());
    }

    @Test(timeout = 10000)
    public void earlyGroupIsNotAFileSizeLimitOrAnUnconditionalFlushThreshold() throws Exception
    {
        Fixture f = new Fixture(32);
        CountDownLatch started = new CountDownLatch(2), release = new CountDownLatch(1);
        AtomicInteger puts = new AtomicInteger();
        when(f.s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call -> {
            puts.incrementAndGet(); started.countDown(); assertTrue(release.await(3, TimeUnit.SECONDS));
            return PutObjectResponse.builder().build();
        });
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 32, f.context, 65536, 2))
        {
            try
            {
                output.add(0, batch(1024), f.schema); output.add(1, batch(1024), f.schema);
                for (int i = 2; i < 32; i++) { output.add(i, batch(768), f.schema); }
                assertTrue(started.await(2, TimeUnit.SECONDS));
                for (int i = 0; i < 2; i++) { output.add(2, batch(1024), f.schema); }
                Field field = S3QSAsyncPartitionOutput.class.getDeclaredField("outstandingWrites");
                field.setAccessible(true);
                assertEquals("partial groups must not keep queueing at the early threshold", 2,
                        ((AtomicInteger) field.get(output)).get());
                release.countDown(); output.finish(f.schema, new HashSet<>());
                assertEquals(32, puts.get()); f.verifyEnds(32);
                assertEquals(4 * 1024 + 30 * 768, f.sent.stream().filter(S3QueueMessage::isData).mapToLong(S3QueueMessage::getRowCount).sum());
            }
            finally { release.countDown(); }
        }
    }

    @Test(timeout = 10000)
    public void lowOutputAcrossManyConfiguredPartitionsDoesNotChurnSmallFiles() throws Exception
    {
        Fixture f = new Fixture(16);
        CountDownLatch put = new CountDownLatch(1);
        when(f.s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call -> {
            put.countDown(); return PutObjectResponse.builder().build();
        });
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 16, f.context, 65536, 1))
        {
            output.add(0, batch(1024), f.schema); output.add(1, batch(1024), f.schema);
            assertFalse("a small total output should not trigger early splitting", put.await(100, TimeUnit.MILLISECONDS));
            output.finish(f.schema, new HashSet<>()); f.verifyEnds(16);
            assertEquals(2, f.sent.stream().filter(S3QueueMessage::isData).count());
        }
    }

    @Test(timeout = 10000)
    public void fewPartitionsRetainNormalGroupsInsteadOfChurningSmallFiles() throws Exception
    {
        Fixture f = new Fixture(2);
        CountDownLatch put = new CountDownLatch(1);
        when(f.s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call -> {
            put.countDown(); return PutObjectResponse.builder().build();
        });
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 2, f.context, 65536, 1))
        {
            output.add(0, batch(1024), f.schema); output.add(1, batch(1024), f.schema);
            assertFalse("few partitions should retain their normal target", put.await(100, TimeUnit.MILLISECONDS));
            output.finish(f.schema, new HashSet<>()); f.verifyEnds(2);
            assertEquals(2048, f.sent.stream().filter(S3QueueMessage::isData).mapToLong(S3QueueMessage::getRowCount).sum());
        }
    }

    @Test(timeout = 10000)
    public void producerScratchCacheIsClearedAfterTheWriterPoolTerminates() throws Exception
    {
        Fixture fixture = new Fixture(1);
        S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                fixture.storage, fixture.shuffle, 0, 1, fixture.context, 8192, 1);
        Field field = S3QSAsyncPartitionOutput.class.getDeclaredField("stagingBuffers");
        field.setAccessible(true);
        io.pixelsdb.pixels.common.physical.FixSizedBuffers cache =
                (io.pixelsdb.pixels.common.physical.FixSizedBuffers) field.get(output);
        ThreadPoolExecutor columns = columnPool(output);
        try
        {
            for (int i = 0; i < 20; i++) { output.add(0, batch(2048), fixture.schema); }
            output.finish(fixture.schema, new HashSet<>());
            assertFalse(fixture.sent.isEmpty());
            assertEquals(0, cache.getCachedBufferCount());
            assertTrue(columns.isShutdown());
            assertTrue(columns.awaitTermination(1, TimeUnit.SECONDS));
            assertTrue("production files must reuse their producer's encoder threads",
                    columns.getCompletedTaskCount() > columns.getLargestPoolSize());
        }
        finally { output.close(); }
        assertEquals(0, cache.getCachedBufferCount());
    }

    private static ThreadPoolExecutor columnPool(S3QSAsyncPartitionOutput output) throws Exception
    {
        Field field = S3QSAsyncPartitionOutput.class.getDeclaredField("columnWriterService");
        field.setAccessible(true); return (ThreadPoolExecutor) field.get(output);
    }

    @Test(timeout = 10000)
    public void producerFailureAndCancellationBothRetireOwnedColumnThreads() throws Exception
    {
        for (boolean cancel : new boolean[]{false, true})
        {
            Fixture f = new Fixture(1);
            CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
            when(f.s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call -> {
                entered.countDown(); release.await(); throw new IOException("injected PUT failure");
            });
            S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                    f.storage, f.shuffle, 0, 1, f.context, 8192, 1);
            ThreadPoolExecutor columns = columnPool(output);
            try
            {
                output.add(0, batch(2048), f.schema);
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                assertTrue(columns.getCompletedTaskCount() > 0);
                if (cancel) { f.context.getCancellation().cancel(new IOException("cancelled producer")); }
                release.countDown();
                assertThrows(Exception.class, () -> output.finish(f.schema, new HashSet<>()));
            }
            finally { release.countDown(); output.close(); }
            assertTrue(columns.isShutdown());
            assertTrue(columns.awaitTermination(1, TimeUnit.SECONDS));
            assertTrue(f.sent.stream().noneMatch(S3QueueMessage::isProducerEnd));
            assertTrue(f.context.getCancellation().isQuiescent());
        }
    }

    @Test(timeout = 10000)
    public void waitingForBytesPreservesPendingWhileAnUploadCanMakeProgress() throws Exception
    {
        Fixture f = new Fixture(2);
        VectorizedRowBatch large = batch(2048), small = batch(16);
        PartitionByteBudget budget = new PartitionByteBudget(large.getMemoryUsage() + small.getMemoryUsage());
        CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
        when(f.s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call -> {
            started.countDown();
            assertTrue(release.await(3, TimeUnit.SECONDS));
            return PutObjectResponse.builder().build();
        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 2, f.context, 8192, 1, budget))
        {
            try
            {
                output.add(0, large, f.schema);
                assertTrue(started.await(1, TimeUnit.SECONDS));
                output.add(1, small, f.schema);
                Future<?> waiting = caller.submit(() -> output.add(1, batch(16), f.schema));
                assertThrows(TimeoutException.class, () -> waiting.get(200, TimeUnit.MILLISECONDS));
                Field partitions = S3QSAsyncPartitionOutput.class.getDeclaredField("partitions");
                partitions.setAccessible(true);
                Object pending = ((Object[]) partitions.get(output))[1];
                Field bytes = pending.getClass().getDeclaredField("estimatedBytes");
                bytes.setAccessible(true);
                synchronized (pending) { assertEquals(small.getMemoryUsage(), bytes.getLong(pending)); }
                release.countDown();
                waiting.get(2, TimeUnit.SECONDS);
                output.finish(f.schema, new HashSet<>());
                f.verifyEnds(2);
                assertEquals(2080, f.sent.stream().filter(S3QueueMessage::isData)
                        .mapToLong(S3QueueMessage::getRowCount).sum());
            }
            finally { release.countDown(); }
        }
        finally { stop(caller); }
        assertEquals(0, budget.used());
    }

    @Test(timeout = 10000)
    public void pressureFlushesLargestPendingIntoEveryIdleWriter() throws Exception
    {
        Fixture f = new Fixture(3);
        VectorizedRowBatch pending = batch(128);
        PartitionByteBudget budget = new PartitionByteBudget(2 * pending.getMemoryUsage());
        CountDownLatch started = new CountDownLatch(2), release = new CountDownLatch(1);
        when(f.s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call -> {
            started.countDown();
            assertTrue(release.await(3, TimeUnit.SECONDS));
            return PutObjectResponse.builder().build();
        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 3, f.context, 8192, 2, budget))
        {
            try
            {
                output.add(0, pending, f.schema);
                output.add(1, batch(128), f.schema);
                Future<?> waiting = caller.submit(() -> output.add(2, batch(128), f.schema));
                assertTrue("both partial groups must run before either upload releases bytes", started.await(2, TimeUnit.SECONDS));
                assertFalse(waiting.isDone());
                assertTrue(f.sent.isEmpty());
                release.countDown(); waiting.get(2, TimeUnit.SECONDS);
                output.finish(f.schema, new HashSet<>()); f.verifyEnds(3);
                assertEquals(384, f.sent.stream().filter(S3QueueMessage::isData).mapToLong(S3QueueMessage::getRowCount).sum());
            }
            finally { release.countDown(); }
        }
        finally { stop(caller); }
        assertEquals(0, budget.used());
    }

    @Test(timeout = 10000)
    public void producerUploadsOverlapAndAllDataMustCompleteBeforeAnyEnd() throws Exception
    {
        Fixture f = new Fixture(2);
        CountDownLatch started = new CountDownLatch(2), release = new CountDownLatch(1);
        when(f.s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call -> {
            started.countDown();
            assertTrue(release.await(3, TimeUnit.SECONDS));
            return PutObjectResponse.builder().build();
        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 2, f.context, 8192, 2))
        {
            try
            {
                output.add(0, batch(2048), f.schema);
                output.add(1, batch(2048), f.schema);
                assertTrue("both uploads must start before either finishes", started.await(2, TimeUnit.SECONDS));
                Future<?> finish = caller.submit(() -> { output.finish(f.schema, new HashSet<>()); return null; });
                assertThrows(TimeoutException.class, () -> finish.get(100, TimeUnit.MILLISECONDS));
                assertTrue(f.sent.isEmpty());
                release.countDown();
                finish.get(3, TimeUnit.SECONDS);
                f.verifyEnds(2);
            }
            finally { release.countDown(); }
        }
        finally { stop(caller); }
        assertTrue(f.context.getCancellation().isQuiescent());
    }

    @Test(timeout = 10000)
    public void sharedBytesFlushPendingPartitionsAndBlockBehindSlowUpload() throws Exception
    {
        Fixture f = new Fixture(2);
        VectorizedRowBatch a = batch(16), b = batch(16), c = batch(16);
        long bytes = a.getMemoryUsage();
        PartitionByteBudget budget = new PartitionByteBudget(bytes * 2);
        CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
        when(f.s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call ->
        {
            started.countDown(); assertTrue(release.await(3, TimeUnit.SECONDS));
            return PutObjectResponse.builder().build();
        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 2, f.context, 8192, 1, budget))
        {
            try
            {
                output.add(0, a, f.schema); output.add(1, b, f.schema);
                Future<?> third = caller.submit(() -> output.add(0, c, f.schema));
                assertTrue(started.await(1, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> third.get(100, TimeUnit.MILLISECONDS));
                assertEquals(bytes * 2, budget.peak());
                release.countDown(); third.get(2, TimeUnit.SECONDS);
                output.finish(f.schema, new HashSet<>());
                assertEquals(0, budget.used());
                f.verifyEnds(2);
            }
            finally { release.countDown(); }
        }
        finally { stop(caller); }
        assertEquals(0, budget.used());
    }

    @Test(timeout = 10000)
    public void moreThanEightWritesQueueUntilByteBudgetIsFull() throws Exception
    {
        Fixture f = new Fixture(1);
        long bytes = batch(2048).getMemoryUsage();
        PartitionByteBudget budget = new PartitionByteBudget(bytes * 12);
        CountDownLatch started = new CountDownLatch(2), release = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger(), peak = new AtomicInteger();
        when(f.s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call ->
        {
            peak.accumulateAndGet(active.incrementAndGet(), Math::max);
            started.countDown();
            try { assertTrue(release.await(5, TimeUnit.SECONDS)); }
            finally { active.decrementAndGet(); }
            return PutObjectResponse.builder().build();
        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 1, f.context, 8192, 2, budget))
        {
            try
            {
                Future<?> queued = caller.submit(() ->
                {
                    for (int i = 0; i < 12; i++) { output.add(0, batch(2048), f.schema); }
                });
                assertTrue(started.await(2, TimeUnit.SECONDS));
                // Both writers are blocked, but submission must pass the former eight-job limit.
                queued.get(2, TimeUnit.SECONDS);
                assertEquals(budget.limit(), budget.used());
                Future<?> next = caller.submit(() -> output.add(0, batch(2048), f.schema));
                assertThrows(TimeoutException.class, () -> next.get(100, TimeUnit.MILLISECONDS));
                assertTrue(f.sent.isEmpty());
                release.countDown();
                next.get(2, TimeUnit.SECONDS);
                output.finish(f.schema, new HashSet<>());
                assertEquals(2, peak.get());
                assertEquals(0, budget.used());
                assertEquals(budget.limit(), budget.peak());
                assertEquals(13 * 2048,
                        f.sent.stream().filter(S3QueueMessage::isData).mapToLong(S3QueueMessage::getRowCount).sum());
                f.verifyEnds(1);
            }
            finally { release.countDown(); }
        }
        finally { stop(caller); }
        assertEquals(0, budget.used());
        assertTrue(f.context.getCancellation().isQuiescent());
    }

    @Test(timeout = 10000)
    public void concurrentScannersMakeProgressWithOnlyThreeBatchesOfSharedSpace() throws Exception
    {
        Fixture f = new Fixture(4);
        PartitionByteBudget budget = new PartitionByteBudget(batch(16).getMemoryUsage() * 3);
        ExecutorService callers = Executors.newFixedThreadPool(4);
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 4, f.context, 8192, 1, budget))
        {
            List<Future<?>> futures = new ArrayList<>();
            for (int scanner = 0; scanner < 4; scanner++)
            {
                final int id = scanner;
                futures.add(callers.submit(() -> {
                    for (int i = 0; i < 20; i++) { output.add((id + i) % 4, batch(16), f.schema); }
                }));
            }
            for (Future<?> future : futures) { future.get(5, TimeUnit.SECONDS); }
            output.finish(f.schema, new HashSet<>());
            assertEquals(0, budget.used());
            assertTrue(budget.peak() <= budget.limit());
            assertEquals(4 * 20 * 16, f.sent.stream().filter(S3QueueMessage::isData)
                    .mapToLong(S3QueueMessage::getRowCount).sum());
            f.verifyEnds(4);
        }
        finally { stop(callers); }
        assertEquals(0, budget.used());
    }

    @Test(timeout = 5000)
    public void closingWhileScannerOwnsBytesRejectsLateTransfer() throws Exception
    {
        Fixture f = new Fixture(1);
        PartitionByteBudget budget = new PartitionByteBudget(batch(16).getMemoryUsage());
        S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 1, f.context, 8192, 1, budget);
        Field closed = S3QSAsyncPartitionOutput.class.getDeclaredField("closed");
        closed.setAccessible(true);
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try
        {
            Future<?> add, close;
            synchronized (output)
            {
                add = callers.submit(() -> output.add(0, batch(16), f.schema));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (budget.used() == 0 && System.nanoTime() < deadline) { Thread.sleep(1); }
                assertEquals(budget.limit(), budget.used());
                close = callers.submit(output::close);
                while (!closed.getBoolean(output) && System.nanoTime() < deadline) { Thread.sleep(1); }
                assertTrue(closed.getBoolean(output));
            }
            assertThrows(ExecutionException.class, () -> add.get(1, TimeUnit.SECONDS));
            close.get(1, TimeUnit.SECONDS);
            assertEquals(0, budget.used());
            assertTrue(f.sent.isEmpty());
        }
        finally { stop(callers); output.close(); }
    }

    @Test(timeout = 5000)
    public void failedPublicationReleasesSharedBytesWithoutEnd() throws Exception
    {
        Fixture f = new Fixture(2);
        PartitionByteBudget budget = new PartitionByteBudget(batch(16).getMemoryUsage() * 3);
        when(f.sqs.sendMessage(any(SendMessageRequest.class)))
                .thenThrow(SqsException.builder().message("injected DATA send failure").build());
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 2, f.context, 8192, 1, budget))
        {
            output.add(0, batch(16), f.schema);
            output.add(1, batch(16), f.schema);
            assertThrows(Exception.class, () -> output.finish(f.schema, new HashSet<>()));
        }
        assertEquals(0, budget.used());
        assertTrue(f.sent.isEmpty());
        assertTrue(f.context.getCancellation().isQuiescent());
    }

    @Test(timeout = 10000)
    public void realProducerContinuesScanningWhileUploadIsBlocked() throws Exception
    {
        Fixture f = new Fixture(1);
        CountDownLatch uploadStarted = new CountDownLatch(1), scanAdvanced = new CountDownLatch(1), release = new CountDownLatch(1);
        when(f.s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call -> {
            uploadStarted.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return PutObjectResponse.builder().build();
        });
        BasePartitionWorker worker = new BasePartitionWorker(f.context)
        {
            @Override
            protected void partitionFile(long trans, long timestamp, List<InputInfo> inputs, String[] columns,
                    Storage.Scheme scheme, TableScanFilter filter, int[] keys, boolean[] projection,
                    List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitions,
                    AtomicReference<TypeDescription> schema, S3QSAsyncPartitionOutput output)
            {
                schema.set(f.schema);
                output.add(0, batch(1048576), f.schema);
                try { assertTrue(uploadStarted.await(3, TimeUnit.SECONDS)); }
                catch (InterruptedException e) { throw new WorkerException("interrupted test scanner", e); }
                // Simulate the next scan result while the first object's PUT has not returned.
                output.add(0, batch(1), f.schema);
                scanAdvanced.countDown();
            }
        };
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (CachedStorage ignored = new CachedStorage(f.storage))
        {
            Future<PartitionOutput> result = caller.submit(() -> worker.process(f.input()));
            try
            {
                boolean advanced = scanAdvanced.await(4, TimeUnit.SECONDS);
                assertTrue(result.isDone() ? result.get().getErrorMessage() : "scanner did not advance", advanced);
                assertFalse(result.isDone());
                assertTrue(f.sent.isEmpty());
                release.countDown();
                PartitionOutput output = result.get(4, TimeUnit.SECONDS);
                assertTrue(output.getErrorMessage(), output.isSuccessful());
                f.verifyEnds(1);
            }
            finally { release.countDown(); }
        }
        finally { stop(caller); }
        assertTrue(f.context.getCancellation().isQuiescent());
    }

    @Test(timeout = 5000)
    public void dataPublishFailureIsReportedWithoutEnd() throws Exception
    {
        Fixture f = new Fixture(1);
        when(f.sqs.sendMessage(any(SendMessageRequest.class)))
                .thenThrow(SqsException.builder().message("injected DATA send failure").build());
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 1, f.context, 8192, 2))
        {
            output.add(0, batch(1), f.schema);
            assertThrows(IOException.class, () -> output.finish(f.schema, new HashSet<>()));
        }
        assertTrue(f.sent.isEmpty());
        verify(f.sqs, times(1)).sendMessage(any(SendMessageRequest.class));
        assertTrue(f.context.getCancellation().isQuiescent());
    }

    @Test(timeout = 5000)
    public void realProducerReportsBackgroundUploadFailureInsteadOfSuccess() throws Exception
    {
        Fixture f = new Fixture(1);
        when(f.s3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(new IllegalStateException("injected background PUT failure"));
        BasePartitionWorker worker = new BasePartitionWorker(f.context)
        {
            @Override
            protected void partitionFile(long trans, long timestamp, List<InputInfo> inputs, String[] columns,
                    Storage.Scheme scheme, TableScanFilter filter, int[] keys, boolean[] projection,
                    List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitions,
                    AtomicReference<TypeDescription> schema, S3QSAsyncPartitionOutput output)
            {
                schema.set(f.schema);
                output.add(0, batch(1), f.schema);
            }
        };
        try (CachedStorage ignored = new CachedStorage(f.storage))
        {
            PartitionOutput result = worker.process(f.input());
            assertFalse(result.isSuccessful());
            assertTrue(result.getErrorMessage(), result.getErrorMessage().contains("background PUT failure"));
            assertTrue(f.sent.isEmpty());
        }
        assertTrue(f.context.getCancellation().isQuiescent());
    }

    @Test(timeout = 10000)
    public void concurrentChunksOfOnePartitionHaveUniqueCompleteSequence() throws Exception
    {
        Fixture f = new Fixture(64);
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                f.storage, f.shuffle, 0, 64, f.context, 65536, 4))
        {
            for (int i = 0; i < 12; ++i) { output.add(0, batch(2048), f.schema); }
            output.finish(f.schema, new HashSet<>());
        }
        f.verifyEnds(64);
        assertEquals(12 * 2048, f.sent.stream().filter(S3QueueMessage::isData).mapToLong(S3QueueMessage::getRowCount).sum());
        int[] occurrences = new int[2048];
        for (S3QueueMessage message : f.sent)
        {
            if (!message.isData()) { continue; }
            try (PixelsReader reader = PixelsReaderImpl.newBuilder().setStorage(f.storage)
                    .setPath(message.getObjectPath()).setPixelsFooterCache(new PixelsFooterCache()).build();
                 PixelsRecordReader records = reader.read(new PixelsReaderOption().includeCols(new String[]{"k"})))
            {
                while (true)
                {
                    VectorizedRowBatch rows = records.readBatch(1024);
                    for (int i = 0; i < rows.size; ++i) { occurrences[(int) ((LongColumnVector) rows.cols[0]).vector[i]]++; }
                    if (rows.endOfFile) { break; }
                }
            }
        }
        for (int count : occurrences) { assertEquals(12, count); }
        assertTrue(f.context.getCancellation().isQuiescent());
    }

    @Test(timeout = 5000)
    public void consumerContinuesProcessingButCannotReturnBeforeAckCompletes() throws Exception
    {
        Fixture f = new Fixture(1);
        CountDownLatch ackStarted = new CountDownLatch(1), processedSecond = new CountDownLatch(1), release = new CountDownLatch(1);
        when(f.sqs.deleteMessage(any(DeleteMessageRequest.class))).thenAnswer(call -> {
            if (((DeleteMessageRequest) call.getArgument(0)).receiptHandle().equals("r0"))
            {
                ackStarted.countDown();
                assertTrue(release.await(3, TimeUnit.SECONDS));
            }
            return DeleteMessageResponse.builder().build();
        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (CachedStorage ignored = new CachedStorage(f.storage))
        {
            Future<?> drain = caller.submit(() -> {
                AtomicInteger handled = new AtomicInteger();
                BasePartitionedJoinWorker.drainS3QSPartition(f.shuffle, 0, pending(data(0), data(1), end(2)), message -> {
                    if (handled.incrementAndGet() == 2)
                    {
                        assertTrue(ackStarted.await(1, TimeUnit.SECONDS));
                        processedSecond.countDown();
                    }
                }, f.context.getCancellation());
                return null;
            });
            try
            {
                assertTrue(processedSecond.await(2, TimeUnit.SECONDS));
                assertFalse(drain.isDone());
                release.countDown();
                drain.get(2, TimeUnit.SECONDS);
                verify(f.sqs, times(3)).deleteMessage(any(DeleteMessageRequest.class));
                verify(f.sqs, never()).receiveMessage(any(ReceiveMessageRequest.class));
            }
            finally { release.countDown(); }
        }
        finally { stop(caller); }
        assertTrue(f.context.getCancellation().isQuiescent());
    }

    @Test(timeout = 5000)
    public void failedAckErrorCodeCannotProduceSuccessfulDrain() throws Exception
    {
        Fixture f = new Fixture(1);
        when(f.sqs.deleteMessage(any(DeleteMessageRequest.class)))
                .thenThrow(SqsException.builder().message("injected delete failure").build());
        try (CachedStorage ignored = new CachedStorage(f.storage))
        {
            Exception failure = assertThrows(Exception.class, () -> BasePartitionedJoinWorker.drainS3QSPartition(
                    f.shuffle, 0, pending(data(0), end(1)), message -> {}, f.context.getCancellation()));
            while (failure.getCause() instanceof Exception) { failure = (Exception) failure.getCause(); }
            assertTrue(failure.getMessage(), failure.getMessage().contains("failed to acknowledge"));
            verify(f.sqs, never()).receiveMessage(any(ReceiveMessageRequest.class));
        }
        assertTrue(f.context.getCancellation().isQuiescent());
    }

    @Test(timeout = 5000)
    public void dataProcessingFailureDoesNotAckThatData() throws Exception
    {
        Fixture f = new Fixture(1);
        try (CachedStorage ignored = new CachedStorage(f.storage))
        {
            assertThrows(IOException.class, () -> BasePartitionedJoinWorker.drainS3QSPartition(
                    f.shuffle, 0, pending(data(0), end(1)), message -> { throw new IOException("data failed"); }));
        }
        verify(f.sqs, never()).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test(timeout = 5000)
    public void redeliveryCannotOverwriteAnEarlierAsyncAcksReceipt() throws Exception
    {
        Fixture f = new Fixture(1);
        CountDownLatch firstDelete = new CountDownLatch(1), release = new CountDownLatch(1);
        Queue<String> receipts = new ConcurrentLinkedQueue<>();
        when(f.sqs.deleteMessage(any(DeleteMessageRequest.class))).thenAnswer(call -> {
            String receipt = ((DeleteMessageRequest) call.getArgument(0)).receiptHandle();
            if (receipt.equals("r0"))
            {
                firstDelete.countDown();
                assertTrue(release.await(3, TimeUnit.SECONDS));
            }
            receipts.add(receipt);
            return DeleteMessageResponse.builder().build();
        });
        S3QueueMessage shared = data(0);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (CachedStorage ignored = new CachedStorage(f.storage))
        {
            AtomicInteger handled = new AtomicInteger();
            Future<?> drain = caller.submit(() -> {
                BasePartitionedJoinWorker.drainS3QSPartition(f.shuffle, 0, pending(shared, shared, end(1)),
                        message -> handled.incrementAndGet());
                return null;
            });
            try
            {
                assertTrue(firstDelete.await(1, TimeUnit.SECONDS));
                release.countDown();
                drain.get(2, TimeUnit.SECONDS);
                assertEquals(1, handled.get());
                assertEquals(new HashSet<>(Arrays.asList("r0", "r1", "r2")), new HashSet<>(receipts));
                assertEquals(3, receipts.size());
                verify(f.sqs, never()).receiveMessage(any(ReceiveMessageRequest.class));
            }
            finally { release.countDown(); }
        }
        finally { stop(caller); }
    }

    @Test(timeout = 5000)
    public void cancellationStopsAnOutstandingAckWithoutSuccessfulDrain() throws Exception
    {
        Fixture f = new Fixture(1);
        CountDownLatch ackStarted = new CountDownLatch(1);
        when(f.sqs.deleteMessage(any(DeleteMessageRequest.class))).thenAnswer(call -> {
            ackStarted.countDown();
            new CountDownLatch(1).await();
            return DeleteMessageResponse.builder().build();
        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (CachedStorage ignored = new CachedStorage(f.storage))
        {
            Future<?> drain = caller.submit(() -> {
                BasePartitionedJoinWorker.drainS3QSPartition(f.shuffle, 0, pending(data(0), end(1)),
                        message -> {}, f.context.getCancellation());
                return null;
            });
            assertTrue(ackStarted.await(1, TimeUnit.SECONDS));
            f.context.getCancellation().cancel(new IOException("cancelled pending ack"));
            assertThrows(ExecutionException.class, () -> drain.get(2, TimeUnit.SECONDS));
        }
        finally { stop(caller); }
        assertTrue(f.context.getCancellation().isQuiescent());
    }

    private static void stop(ExecutorService executor) throws InterruptedException
    {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
    }

    private static TypeDescription schema()
    {
        return TypeDescription.createStruct().addField("k", TypeDescription.createLong());
    }

    private static VectorizedRowBatch batch(int size)
    {
        VectorizedRowBatch batch = schema().createRowBatch(size);
        for (int i = 0; i < size; ++i) { ((LongColumnVector) batch.cols[0]).vector[i] = i; }
        batch.size = size;
        return batch;
    }

    private static S3QueueMessage data(long sequence)
    {
        return S3QueueMessage.data("async", 0, 0, 0, sequence, "bucket/object");
    }

    private static S3QueueMessage end(long sequence) { return S3QueueMessage.producerEnd("async", 0, 0, 0, sequence); }

    private static Map<Integer, Queue<S3QueuePollResult>> pending(S3QueueMessage... messages)
    {
        Queue<S3QueuePollResult> queue = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < messages.length; ++i) { queue.add(new S3QueuePollResult("r" + i, messages[i])); }
        Map<Integer, Queue<S3QueuePollResult>> result = new HashMap<>();
        result.put(0, queue);
        return result;
    }

    private static final class CachedStorage implements AutoCloseable
    {
        final Field field;
        final Object previous;
        CachedStorage(S3QS storage) throws Exception
        {
            field = WorkerCommon.class.getDeclaredField("s3qs");
            field.setAccessible(true);
            previous = field.get(null);
            field.set(null, storage);
        }
        @Override public void close() throws Exception { field.set(null, previous); }
    }

    private static final class Fixture
    {
        final S3Client s3 = mock(S3Client.class);
        final SqsClient sqs = mock(SqsClient.class);
        final S3QS storage = new S3QS(s3, sqs, 30);
        final Queue<S3QueueMessage> sent = new ConcurrentLinkedQueue<>();
        final Map<String, byte[]> objects = new ConcurrentHashMap<>();
        final TypeDescription schema = schema();
        final StorageInfo storageInfo = new StorageInfo(Storage.Scheme.s3qs, null, null, null, null);
        final ShuffleInfo shuffle;
        final WorkerContext context = new WorkerContext(mock(Logger.class), new WorkerMetrics(), "async")
                .withCancellation(new WorkerCancellation());
        Fixture(int partitions) throws Exception
        {
            List<ShuffleQueueInfo> queues = new ArrayList<>();
            for (int i = 0; i < partitions; ++i) { queues.add(new ShuffleQueueInfo(i, "ignored", "queue" + i)); }
            shuffle = new ShuffleInfo("async", storageInfo, "bucket/shuffle/", partitions, 1, 1, 1, queues);
            for (int i = 0; i < partitions; ++i) { storage.registerQueue("async", i, "ignored", "queue" + i); }
            when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call -> {
                PutObjectRequest request = call.getArgument(0);
                RequestBody body = call.getArgument(1);
                byte[] bytes = new byte[(int) body.contentLength()];
                try (java.io.DataInputStream input = new java.io.DataInputStream(body.contentStreamProvider().newStream()))
                {
                    input.readFully(bytes);
                }
                objects.put(request.bucket() + "/" + request.key(), bytes);
                return PutObjectResponse.builder().build();
            });
            when(s3.getObject(any(GetObjectRequest.class),
                    org.mockito.ArgumentMatchers.<ResponseTransformer<GetObjectResponse, ResponseBytes<GetObjectResponse>>>any()))
                    .thenAnswer(call -> {
                        GetObjectRequest request = call.getArgument(0);
                        return ResponseBytes.fromByteArray(GetObjectResponse.builder().build(),
                                objects.get(request.bucket() + "/" + request.key()));
                    });
            when(sqs.sendMessage(any(SendMessageRequest.class))).thenAnswer(call -> {
                sent.add(S3QueueMessage.fromMessageBody(((SendMessageRequest) call.getArgument(0)).messageBody()));
                return SendMessageResponse.builder().build();
            });
            when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder().build());
        }
        PartitionInput input()
        {
            ScanTableInfo table = new ScanTableInfo("test", true, new String[]{"k"}, storageInfo,
                    Collections.singletonList(new InputSplit(Collections.singletonList(new InputInfo("unused", 0, 1)))),
                    JSON.toJSONString(TableScanFilter.empty("test", "test")));
            OutputInfo output = new OutputInfo("bucket/shuffle/", storageInfo, false);
            output.setShuffleInfo(shuffle);
            PartitionInput input = new PartitionInput(1, 1, table, new boolean[]{true}, output, new PartitionInfo(new int[]{0}, 1));
            input.setProducerTaskId(0);
            return input;
        }
        void verifyEnds(int expected)
        {
            Map<Integer, Set<Long>> sequences = new HashMap<>();
            boolean ended = false;
            int ends = 0;
            for (S3QueueMessage message : sent)
            {
                if (message.isData())
                {
                    assertFalse("no DATA may follow any END", ended);
                    assertTrue(sequences.computeIfAbsent(message.getPartitionId(), k -> new HashSet<>()).add(message.getSequenceId()));
                }
                else
                {
                    ended = true;
                    ++ends;
                    Set<Long> data = sequences.getOrDefault(message.getPartitionId(), Collections.emptySet());
                    assertEquals(data.size(), message.getSequenceId());
                    for (long i = 0; i < data.size(); ++i) { assertTrue(data.contains(i)); }
                }
            }
            assertEquals(expected, ends);
        }
    }
}
