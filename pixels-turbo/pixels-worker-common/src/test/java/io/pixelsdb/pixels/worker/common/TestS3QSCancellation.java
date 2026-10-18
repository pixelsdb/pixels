/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestS3QSCancellation
{
    @Test(timeout = 5000)
    public void taskFailureIsPublishedBeforePoolTerminationEvenWhenLoggingBlocks() throws Exception
    {
        org.apache.logging.log4j.Logger logger = mock(org.apache.logging.log4j.Logger.class);
        RuntimeException original = new RuntimeException("partition child failed");
        CountDownLatch logging = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(call ->
        {
            logging.countDown();
            boolean interrupted = false;
            while (true)
            {
                try { release.await(); break; }
                catch (InterruptedException expected) { interrupted = true; }
            }
            if (interrupted) { Thread.currentThread().interrupt(); }
            return null;
        })
                .when(logger).error(anyString(), same(original));
        WorkerCancellation cancellation = new WorkerCancellation();
        WorkerContext context = new WorkerContext(logger, new WorkerMetrics(), "failure-order").withCancellation(cancellation);
        WorkerThreadExceptionHandler handler = new WorkerThreadExceptionHandler(logger, cancellation);
        ExecutorService pool = context.createThreadPool(1, handler);
        try
        {
            pool.execute(() -> { throw original; });
            pool.shutdown();
            assertTrue(logging.await(1, TimeUnit.SECONDS));
            assertSame(original, handler.getException());
            assertTrue(handler.hasException());
            assertFalse("afterExecute must run before termination is observable", pool.isTerminated());
        }
        finally { release.countDown(); pool.shutdownNow(); assertTrue(pool.awaitTermination(1, TimeUnit.SECONDS)); }
    }

    @Test(timeout = 5000)
    public void consumerMissingEndStopsAtPollCheckpoint() throws Exception { missingEnd(false); }

    @Test(timeout = 5000)
    public void chainConsumerMissingEndStopsThroughSharedDrain() throws Exception { missingEnd(true); }

    @Test(timeout = 5000)
    public void uninterruptibleChildIsNotAcknowledgedAsStopped() throws Exception
    {
        WorkerCancellation cancellation = new WorkerCancellation();
        WorkerContext context = new WorkerContext(org.apache.logging.log4j.LogManager.getLogger(getClass()),
                new WorkerMetrics(), "queued-child").withCancellation(cancellation);
        ExecutorService executor = context.createThreadPool(1,
                new WorkerThreadExceptionHandler(context.getLogger(), cancellation));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        executor.submit(() ->
        {
            started.countDown();
            boolean done = false;
            while (!done)
            {
                try { release.await(); done = true; }
                catch (InterruptedException ignored) { /* Simulates I/O that ignores interrupts. */ }
            }
        });
        started.await();
        Future<?> queued = executor.submit(() -> fail("cancelled queued task must not start"));
        try
        {
            cancellation.cancel(new RuntimeException("abort"));
            try { queued.get(); fail("queued future wait must observe cancellation"); }
            catch (WorkerException expected) { assertEquals("abort", expected.getCause().getMessage()); }
            assertFalse(cancellation.stopAndAwait(20));
            assertFalse(cancellation.isQuiescent());
        }
        finally { release.countDown(); executor.awaitTermination(1, TimeUnit.SECONDS); }
        assertTrue(cancellation.isQuiescent());
    }

    @Test
    public void nextInvocationDoesNotInheritPreviousCancellation()
    {
        WorkerContext base = new WorkerContext(null, new WorkerMetrics(), "warm-worker");
        WorkerContext first = base.withCancellation(new WorkerCancellation());
        first.getCancellation().cancel(new RuntimeException("query one"));
        WorkerContext second = base.withCancellation(new WorkerCancellation());
        second.getCancellation().check();
        base.getCancellation().check();
    }

    private static void missingEnd(boolean chain) throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3QS storage = new S3QS(mock(S3Client.class), sqs, 30);
        storage.registerQueue("cancel-shuffle", 0, "ignored", "queue-url");
        Field field = WorkerCommon.class.getDeclaredField("s3qs");
        field.setAccessible(true);
        Object previous = field.get(null);
        field.set(null, storage);
        CountDownLatch polling = new CountDownLatch(1);
        when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(call ->
        {
            ReceiveMessageRequest request = call.getArgument(0);
            assertTrue(request.waitTimeSeconds() <= 2);
            polling.countDown();
            Thread.sleep(10);
            return ReceiveMessageResponse.builder().build();
        });
        WorkerCancellation cancellation = new WorkerCancellation();
        ShuffleInfo shuffle = new ShuffleInfo("cancel-shuffle",
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                "bucket/cancel-shuffle/", 1, 1, 20, 1, Collections.emptyList());
        RuntimeException original = new RuntimeException("producer failed before END");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> consumer = executor.submit(() ->
        {
            try
            {
                if (chain)
                {
                    BasePartitionedChainJoinWorker.joinWithRightTableS3QS(1, 1, null, null,
                            shuffle, new HashMap<>(), new String[0], Collections.singletonList(0),
                            new ConcurrentLinkedQueue<>(), new WorkerMetrics(), cancellation);
                }
                else
                {
                    BasePartitionedJoinWorker.drainS3QSPartition(shuffle, 0, new HashMap<>(), message -> {}, cancellation);
                }
                fail("missing END must wait for cancellation");
            }
            catch (WorkerException expected) { assertSame(original, expected.getCause()); }
            catch (Exception unexpected) { throw new AssertionError(unexpected); }
        });
        try
        {
            assertTrue(polling.await(1, TimeUnit.SECONDS));
            cancellation.cancel(original);
            consumer.get(1, TimeUnit.SECONDS);
            verify(sqs, never()).deleteMessage(any(software.amazon.awssdk.services.sqs.model.DeleteMessageRequest.class));
        }
        finally
        {
            cancellation.cancel(original);
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.SECONDS);
            field.set(null, previous);
            storage.close();
        }
    }
}
