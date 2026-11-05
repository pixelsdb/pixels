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
 * Affero General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleQueueInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestS3QSQueuePreparation
{
    @Test(timeout = 10000)
    public void createsInBoundedParallelWithCompleteIntentAndOnlyTwoProgressWrites() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3QS storage = storage(sqs);
        S3QSShuffleResourceLifecycle lifecycle = new S3QSShuffleResourceLifecycle(storage);
        ShuffleInfo info = info(24);
        AtomicReference<Map<String, Object>> snapshot = new AtomicReference<>();
        AtomicInteger progressWrites = new AtomicInteger();
        lifecycle.setProgressListener(value -> { snapshot.set(value); progressWrites.incrementAndGet(); });
        progressWrites.set(0); // Exclude listener installation.
        CountDownLatch firstWave = new CountDownLatch(16), release = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger(), peak = new AtomicInteger();
        when(sqs.createQueue(any(CreateQueueRequest.class))).thenAnswer(call -> {
            assertEquals(24, ((List<?>) snapshot.get().get("ownedQueues")).size());
            int count = active.incrementAndGet();
            peak.accumulateAndGet(count, Math::max);
            firstWave.countDown();
            try
            {
                assertTrue(release.await(3, TimeUnit.SECONDS));
                return CreateQueueResponse.builder()
                        .queueUrl("url/" + ((CreateQueueRequest) call.getArgument(0)).queueName()).build();
            }
            finally { active.decrementAndGet(); }
        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        Future<?> preparation = caller.submit(() -> {
            lifecycle.prepare(Collections.singletonList(info));
            return null;
        });
        try
        {
            assertTrue(firstWave.await(3, TimeUnit.SECONDS));
            assertFalse(preparation.isDone());
            assertEquals(1, progressWrites.get());
            release.countDown();
            preparation.get(3, TimeUnit.SECONDS);
            assertEquals(16, peak.get());
            assertEquals(2, progressWrites.get());
            for (ShuffleQueueInfo queue : info.getQueues())
            {
                assertEquals("url/" + queue.getQueueName(), queue.getQueueUrl());
            }
            verify(sqs, times(24)).createQueue(any(CreateQueueRequest.class));
            verify(sqs, never()).getQueueUrl(any(GetQueueUrlRequest.class));
        }
        finally
        {
            release.countDown();
            caller.shutdownNow();
            assertTrue(caller.awaitTermination(3, TimeUnit.SECONDS));
            storage.close();
        }
    }

    @Test(timeout = 10000)
    public void cancellationWaitsForCreateThatIgnoresInterruptionBeforeCleanup() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3QS storage = storage(sqs);
        S3QSShuffleResourceLifecycle lifecycle = new S3QSShuffleResourceLifecycle(storage);
        CountDownLatch entered = new CountDownLatch(2), release = new CountDownLatch(1);
        CountDownLatch createInterrupted = new CountDownLatch(1), finished = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean restoredInterrupt = new AtomicBoolean();
        when(sqs.createQueue(any(CreateQueueRequest.class))).thenAnswer(call -> {
            entered.countDown();
            while (true)
            {
                try { release.await(); break; }
                catch (InterruptedException ignored) { createInterrupted.countDown(); }
            }
            return CreateQueueResponse.builder()
                    .queueUrl("url/" + ((CreateQueueRequest) call.getArgument(0)).queueName()).build();
        });
        Thread caller = new Thread(() -> {
            try { lifecycle.prepare(Collections.singletonList(info(2))); }
            catch (Throwable caught) { failure.set(caught); }
            finally { restoredInterrupt.set(Thread.currentThread().isInterrupted()); finished.countDown(); }
        });
        caller.start();
        try
        {
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            caller.interrupt();
            assertTrue(createInterrupted.await(3, TimeUnit.SECONDS));
            assertEquals(1, finished.getCount());
            verify(sqs, never()).deleteQueue(any(DeleteQueueRequest.class));
            release.countDown();
            assertTrue(finished.await(3, TimeUnit.SECONDS));
            assertTrue(failure.get() instanceof IOException);
            assertTrue(restoredInterrupt.get());
            verify(sqs, never()).deleteQueue(any(DeleteQueueRequest.class));
            lifecycle.cleanup();
            verify(sqs, times(2)).deleteQueue(any(DeleteQueueRequest.class));
            verify(sqs, never()).getQueueUrl(any(GetQueueUrlRequest.class));
        }
        finally
        {
            release.countDown();
            caller.join(3000);
            assertFalse(caller.isAlive());
            storage.close();
        }
    }

    @Test
    public void recordedButUncreatedQueueDoesNotMakeFailureCleanupFail() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3QS storage = storage(sqs);
        SqsException.Builder rejection = SqsException.builder();
        rejection.message("create rejected");
        SqsException rejected = (SqsException) rejection.build();
        when(sqs.createQueue(any(CreateQueueRequest.class))).thenThrow(rejected);
        when(sqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenThrow(
                QueueDoesNotExistException.builder().message("not created").build());
        AtomicReference<Map<String, Object>> snapshot = new AtomicReference<>();
        S3QSShuffleResourceLifecycle lifecycle = new S3QSShuffleResourceLifecycle(storage);
        lifecycle.setProgressListener(snapshot::set);
        try
        {
            lifecycle.prepare(Collections.singletonList(info(1)));
            fail("prepare must fail");
        }
        catch (IOException expected)
        {
            assertSame(rejected, expected.getCause());
            assertEquals(0, expected.getSuppressed().length);
        }
        lifecycle.cleanup();
        assertEquals(true, snapshot.get().get("cleaned"));
        verify(sqs, never()).deleteQueue(any(DeleteQueueRequest.class));
        storage.close();
    }

    private static S3QS storage(SqsClient sqs)
    {
        S3Client s3 = mock(S3Client.class);
        when(s3.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder().keyCount(0).build());
        return new S3QS(s3, sqs, 30);
    }

    private static ShuffleInfo info(int count)
    {
        List<ShuffleQueueInfo> queues = new ArrayList<>();
        for (int i = 0; i < count; i++) { queues.add(new ShuffleQueueInfo(i, "query-p" + i, null)); }
        return new ShuffleInfo("query", new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                "bucket/query/", count, 1, 1, 1, queues);
    }
}
