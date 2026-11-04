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

import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.physical.scheduler.*;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.reader.PixelsRecordReaderImpl;
import io.pixelsdb.pixels.storage.s3.*;
import org.junit.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

/** Offline failure injection at the actual scheduler / reader / SDK boundaries. */
public class TestS3ReadLifecycle
{
    @Test(timeout = 10000)
    public void allSchedulersPropagateFailuresNullResponsesAndSubmissionErrors() throws Exception
    {
        ConfigFactory.Instance().addProperty("read.request.enable.retry", "false");
        for (Scheduler scheduler : Arrays.asList(NoopScheduler.Instance(), SortMergeScheduler.Instance(), RateLimitedScheduler.Instance()))
        {
            for (int mode = 0; mode < 4; mode++)
            {
                PhysicalReader reader = mock(PhysicalReader.class);
                when(reader.supportsAsync()).thenReturn(true);
                when(reader.getPath()).thenReturn("offline");
                CompletableFuture<ByteBuffer> source = new CompletableFuture<>();
                if (mode == 0) { source.completeExceptionally(new IOException("injected")); }
                if (mode == 1) { source.complete(null); }
                if (mode == 3) { source.complete(ByteBuffer.allocate(3)); }
                if (mode == 2) { when(reader.readAsync(anyLong(), anyInt())).thenThrow(new IOException("submit")); }
                else { when(reader.readAsync(anyLong(), anyInt())).thenReturn(source); }
                Scheduler.RequestBatch batch = new Scheduler.RequestBatch();
                CompletableFuture<ByteBuffer> chunk = batch.add(1, 0, 4);
                scheduler.executeBatch(reader, batch, 1);
                assertThrows(ExecutionException.class, () -> chunk.get(1, TimeUnit.SECONDS));
            }
        }
    }

    @Test(timeout = 5000)
    public void cancellingMergedChunkCancelsPhysicalReadAndAllChunks() throws Exception
    {
        ConfigFactory.Instance().addProperty("read.request.merge.gap", "0");
        PhysicalReader reader = mock(PhysicalReader.class);
        when(reader.supportsAsync()).thenReturn(true);
        CompletableFuture<ByteBuffer> source = new CompletableFuture<>();
        when(reader.readAsync(anyLong(), anyInt())).thenReturn(source);
        Scheduler.RequestBatch batch = new Scheduler.RequestBatch();
        CompletableFuture<ByteBuffer> a = batch.add(1, 0, 4), b = batch.add(1, 4, 4);
        SortMergeScheduler.Instance().executeBatch(reader, batch, 1);
        b.cancel(true);
        assertTrue(source.isCancelled());
        assertTrue(a.isCompletedExceptionally());
    }

    @Test(timeout = 5000)
    public void cancellationBeforeMergingCompletesEveryChunkWithoutSubmitting() throws Exception
    {
        PhysicalReader reader = mock(PhysicalReader.class);
        when(reader.supportsAsync()).thenReturn(true);
        Scheduler.RequestBatch batch = new Scheduler.RequestBatch();
        CompletableFuture<ByteBuffer> first = batch.add(1, 0, 4), second = batch.add(1, 4, 4);
        first.cancel(true);
        SortMergeScheduler.Instance().executeBatch(reader, batch, 1);
        assertTrue(second.isCompletedExceptionally());
        verify(reader, never()).readAsync(anyLong(), anyInt());
    }

    @Test(timeout = 5000)
    public void mergedReadPreservesBackendCursorAndReturnsCorrectSlices() throws Exception
    {
        PhysicalReader reader = mock(PhysicalReader.class);
        when(reader.supportsAsync()).thenReturn(true);
        ByteBuffer bytes = ByteBuffer.wrap(new byte[]{9, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        bytes.position(2); bytes.limit(10);
        when(reader.readAsync(0, 8)).thenReturn(CompletableFuture.completedFuture(bytes));
        Scheduler.RequestBatch batch = new Scheduler.RequestBatch();
        CompletableFuture<ByteBuffer> first = batch.add(1, 0, 4), second = batch.add(1, 4, 4);
        SortMergeScheduler.Instance().executeBatch(reader, batch, 1);
        assertEquals(0x01020304, first.get().getInt());
        assertEquals(0x05060708, second.get().getInt());
        assertEquals(2, bytes.position()); assertEquals(10, bytes.limit());
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void closeWaitsForSubmissionAndCancelsItsLateSdkFuture() throws Exception
    {
        S3AsyncClient async = mock(S3AsyncClient.class); S3Client sync = mock(S3Client.class);
        CountDownLatch entered = new CountDownLatch(1), allowReturn = new CountDownLatch(1);
        CompletableFuture<ResponseBytes<GetObjectResponse>> source = new CompletableFuture<>();
        doAnswer(call -> { entered.countDown(); allowReturn.await(); return source; })
                .when(async).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
        ExecutorService workers = Executors.newFixedThreadPool(2);
        PhysicalS3Reader reader = reader(new S3(sync, async), async, sync);
        try
        {
            Future<CompletableFuture<ByteBuffer>> submitting = workers.submit(() -> reader.readAsync(0, 4));
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            Future<?> closing = workers.submit(() -> { reader.close(); return null; });
            assertThrows(TimeoutException.class, () -> closing.get(150, TimeUnit.MILLISECONDS));
            allowReturn.countDown(); closing.get(2, TimeUnit.SECONDS);
            assertTrue(source.isCancelled()); assertTrue(submitting.get().isCancelled());
            assertThrows(IOException.class, () -> reader.readAsync(0, 4));
            verify(async, never()).close();
        }
        finally { allowReturn.countDown(); reader.close(); workers.shutdownNow(); }
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void synchronousCloseRetainsAdmissionUntilCallerActuallyReturns() throws Exception
    {
        S3AsyncClient async = mock(S3AsyncClient.class); S3Client sync = mock(S3Client.class);
        when(sync.headObject(any(HeadObjectRequest.class))).thenReturn(HeadObjectResponse.builder().contentLength(4L).build());
        ConfigFactory.Instance().addProperty("s3.use.async.client", "false");
        CountDownLatch entered = new CountDownLatch(1), allowReturn = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        doAnswer(call -> {
            entered.countDown();
            while (true)
            {
                try { allowReturn.await(); break; }
                catch (InterruptedException ignored) { interrupted.set(true); }
            }
            return ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[4]);
        }).when(sync).getObject(any(GetObjectRequest.class), any(software.amazon.awssdk.core.sync.ResponseTransformer.class));
        ExecutorService workers = Executors.newFixedThreadPool(2);
        PhysicalS3Reader reader = new PhysicalS3Reader(new S3(sync, async), "bucket/input");
        Field slotsField = PhysicalS3Reader.class.getDeclaredField("readSlots"); slotsField.setAccessible(true);
        Semaphore slots = (Semaphore) slotsField.get(null); int limit = slots.availablePermits();
        try
        {
            Future<CompletableFuture<ByteBuffer>> reading = workers.submit(() -> reader.readAsync(0, 4));
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            Future<?> closing = workers.submit(() -> { reader.close(); return null; });
            assertThrows(TimeoutException.class, () -> closing.get(150, TimeUnit.MILLISECONDS));
            assertEquals(limit - 1, slots.availablePermits());
            allowReturn.countDown(); closing.get(2, TimeUnit.SECONDS);
            assertTrue(interrupted.get()); assertTrue(reading.get().isCancelled());
            assertEquals(limit, slots.availablePermits()); verifyNoInteractions(async);
        }
        finally
        {
            allowReturn.countDown(); reader.close(); workers.shutdownNow();
            ConfigFactory.Instance().addProperty("s3.use.async.client", "true");
        }
    }

    @Test(timeout = 5000)
    @SuppressWarnings("unchecked")
    public void completionCallbackCanCloseReaderWithoutWaitingForItself() throws Exception
    {
        S3AsyncClient async = mock(S3AsyncClient.class); S3Client sync = mock(S3Client.class);
        CompletableFuture<ResponseBytes<GetObjectResponse>> source = new CompletableFuture<>();
        when(async.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(source);
        try (PhysicalS3Reader reader = reader(new S3(sync, async), async, sync))
        {
            CompletableFuture<Void> callback = reader.readAsync(0, 4).thenAccept(bytes -> {
                try { reader.close(); }
                catch (IOException failure) { throw new CompletionException(failure); }
            });
            source.complete(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[4]));
            callback.get(1, TimeUnit.SECONDS);
        }
    }

    private static PhysicalS3Reader reader(S3 storage, S3AsyncClient async, S3Client sync) throws Exception
    {
        ConfigFactory.Instance().addProperty("s3.use.async.client", "true");
        ConfigFactory.Instance().addProperty("s3.enable.async", "true");
        when(sync.headObject(any(HeadObjectRequest.class))).thenReturn(
                HeadObjectResponse.builder().contentLength(16L << 20).build());
        return new PhysicalS3Reader(storage, "bucket/input");
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void admissionIsSharedAcrossReadersAndInterruptedWaitDoesNotSubmit() throws Exception
    {
        S3AsyncClient async = mock(S3AsyncClient.class);
        S3Client sync = mock(S3Client.class); S3 storage = new S3(sync, async);
        List<CompletableFuture<ResponseBytes<GetObjectResponse>>> pending = new CopyOnWriteArrayList<>();
        doAnswer(call -> {
            CompletableFuture<ResponseBytes<GetObjectResponse>> future = new CompletableFuture<>();
            pending.add(future); return future;
        }).when(async).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
        PhysicalS3Reader first = reader(storage, async, sync), second = reader(storage, async, sync);
        Field slotsField = PhysicalS3Reader.class.getDeclaredField("readSlots"); slotsField.setAccessible(true);
        Semaphore slots = (Semaphore) slotsField.get(null); int limit = slots.availablePermits();
        assertTrue(limit > 0);
        ExecutorService submit = Executors.newSingleThreadExecutor();
        CountDownLatch waiting = new CountDownLatch(1); AtomicBoolean interrupted = new AtomicBoolean();
        try
        {
            for (int i = 0; i < limit; i++) { (i % 2 == 0 ? first : second).readAsync(0, 4); }
            Future<?> blocked = submit.submit(() -> {
                waiting.countDown();
                try { second.readAsync(0, 4); fail("must not submit without a slot"); }
                catch (IOException failure) { interrupted.set(Thread.currentThread().isInterrupted()); }
            });
            assertTrue(waiting.await(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> blocked.get(150, TimeUnit.MILLISECONDS));
            assertEquals(limit, pending.size());
            submit.shutdownNow(); assertTrue(submit.awaitTermination(2, TimeUnit.SECONDS));
            assertTrue(interrupted.get()); assertEquals(limit, pending.size());
            first.close(); second.close();
            assertTrue(pending.stream().allMatch(CompletableFuture::isCancelled));
            assertEquals(limit, slots.availablePermits());
        }
        finally { first.close(); second.close(); submit.shutdownNow(); }
    }

    @Test(timeout = 5000)
    @SuppressWarnings("unchecked")
    public void sdkFailureKeepsSharedClientsAndLargeReadsUseSameOwnedPath() throws Exception
    {
        S3AsyncClient async = mock(S3AsyncClient.class);
        S3Client sync = mock(S3Client.class); S3 storage = new S3(sync, async);
        AtomicReference<CompletableFuture<ResponseBytes<GetObjectResponse>>> current = new AtomicReference<>();
        doAnswer(call -> {
            CompletableFuture<ResponseBytes<GetObjectResponse>> source = new CompletableFuture<>();
            current.set(source); return source;
        }).when(async).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
        try (PhysicalS3Reader reader = reader(storage, async, sync))
        {
            IOException cause = new IOException("transport failed");
            CompletableFuture<ByteBuffer> failed = reader.readAsync(0, 4);
            current.get().completeExceptionally(cause);
            assertSame(cause, assertThrows(ExecutionException.class, failed::get).getCause());
            CompletableFuture<ByteBuffer> large = reader.readAsync(0, 3 << 20);
            large.cancel(true); assertTrue(current.get().isCancelled());
            storage.reconnect();
            assertSame(async, storage.getS3AsyncClient()); assertSame(sync, storage.getS3Client());
            CompletableFuture<ByteBuffer> success = reader.readAsync(0, 4);
            current.get().complete(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{1, 2, 3, 4}));
            assertEquals(16909060, success.get().getInt());
            verify(sync, never()).getObject(any(GetObjectRequest.class), any(software.amazon.awssdk.core.sync.ResponseTransformer.class));
        }
        verify(async, never()).close();
    }

    @Test(timeout = 5000)
    @SuppressWarnings("unchecked")
    public void synchronousClientFailureCompletesFutureAndReturnsAdmission() throws Exception
    {
        S3AsyncClient async = mock(S3AsyncClient.class); S3Client sync = mock(S3Client.class);
        PhysicalS3Reader reader = reader(new S3(sync, async), async, sync);
        reader.close();
        ConfigFactory.Instance().addProperty("s3.use.async.client", "false");
        try (PhysicalS3Reader synchronous = new PhysicalS3Reader(new S3(sync, async), "bucket/input"))
        {
            when(sync.getObject(any(GetObjectRequest.class), any(software.amazon.awssdk.core.sync.ResponseTransformer.class)))
                    .thenThrow(software.amazon.awssdk.core.exception.SdkClientException.create("sync failure"));
            Field field = PhysicalS3Reader.class.getDeclaredField("readSlots"); field.setAccessible(true);
            Semaphore slots = (Semaphore) field.get(null); int before = slots.availablePermits();
            assertThrows(ExecutionException.class, () -> synchronous.readAsync(0, 4).get());
            assertEquals(before, slots.availablePermits()); verifyNoInteractions(async);
        }
        finally { ConfigFactory.Instance().addProperty("s3.use.async.client", "true"); }
    }

    @Test(timeout = 10000)
    public void windowWaitFailureAndInterruptCancelSiblingsAndCloseReader() throws Exception
    {
        for (boolean interrupt : new boolean[]{false, true})
        {
            PhysicalReader physical = mock(PhysicalReader.class);
            when(physical.supportsAsync()).thenReturn(true);
            List<CompletableFuture<ByteBuffer>> sources = new CopyOnWriteArrayList<>();
            CountDownLatch submitted = new CountDownLatch(2);
            when(physical.readAsync(anyLong(), anyInt())).thenAnswer(call -> {
                CompletableFuture<ByteBuffer> source = new CompletableFuture<>(); sources.add(source);
                submitted.countDown(); return source;
            });
            PixelsRecordReaderImpl records = mock(PixelsRecordReaderImpl.class, CALLS_REAL_METHODS);
            Field field = PixelsRecordReaderImpl.class.getDeclaredField("physicalReader"); field.setAccessible(true); field.set(records, physical);
            Scheduler.RequestBatch batch = new Scheduler.RequestBatch(); List<CompletableFuture> actions = new ArrayList<>();
            actions.add(batch.add(1, 0, 4).thenAccept(value -> fail("failed window must not decode")));
            actions.add(batch.add(1, 1L << 20, 4).thenAccept(value -> fail("failed window must not decode")));
            Method wait = PixelsRecordReaderImpl.class.getDeclaredMethod("readBatchRequests", Scheduler.class, Scheduler.RequestBatch.class, List.class);
            wait.setAccessible(true); AtomicReference<Throwable> failure = new AtomicReference<>(); AtomicBoolean restored = new AtomicBoolean();
            Thread worker = new Thread(() -> {
                try { wait.invoke(records, SortMergeScheduler.Instance(), batch, actions); }
                catch (InvocationTargetException expected) { failure.set(expected.getCause()); }
                catch (Exception unexpected) { failure.set(unexpected); }
                finally { restored.set(Thread.currentThread().isInterrupted()); }
            });
            worker.start(); assertTrue(submitted.await(2, TimeUnit.SECONDS));
            if (interrupt) { worker.interrupt(); }
            else { sources.get(1).completeExceptionally(new IOException("original read failure")); }
            worker.join(2000); assertFalse(worker.isAlive()); assertTrue(failure.get() instanceof IOException);
            assertTrue(sources.get(0).isCancelled()); assertTrue(actions.stream().allMatch(CompletableFuture::isDone));
            if (!interrupt)
            {
                Throwable rootCause = failure.get();
                while (rootCause.getCause() != null) { rootCause = rootCause.getCause(); }
                assertEquals("original read failure", rootCause.getMessage());
            }
            assertEquals(interrupt, restored.get()); verify(physical).close();
        }
    }
}
