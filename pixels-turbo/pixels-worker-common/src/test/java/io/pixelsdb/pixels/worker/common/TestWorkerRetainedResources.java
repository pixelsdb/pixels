/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.storage.s3qs.*;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestWorkerRetainedResources
{
    @Test
    public void metadataEvictsOldEntriesWithoutInvalidatingReaderReferences()
    {
        PixelsFooterCache cache = new PixelsFooterCache(2, 3);
        PixelsProto.FileTail held = PixelsProto.FileTail.getDefaultInstance();
        PixelsProto.RowGroupFooter rg = PixelsProto.RowGroupFooter.getDefaultInstance();
        for (int i = 0; i < 100; i++) { cache.putFileTail("f" + i, held); cache.putRGFooter("r" + i, rg); }
        int files = 0, groups = 0;
        for (int i = 0; i < 100; i++)
        {
            if (cache.getFileTail("f" + i) != null) { files++; }
            if (cache.getRGFooter("r" + i) != null) { groups++; }
        }
        assertTrue(files <= 2); assertTrue(groups <= 3);
        assertSame(held, cache.getFileTail("f99")); assertSame(rg, cache.getRGFooter("r99"));
        assertNotNull(held.getFooter()); assertNotNull(rg.getRowGroupEncoding());
    }

    @Test
    public void overlappingTasksKeepQueueUntilLastReleaseAndDoNotDeleteRemoteResources() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class); S3Client s3 = mock(S3Client.class);
        S3QS storage = new S3QS(s3, sqs, 30);
        storage.retainQueue("shared", 0, "q", "url"); storage.retainQueue("shared", 0, "q", "url");
        storage.releaseQueue("shared", 0);
        assertEquals(1, queues(storage).size());
        storage.releaseQueue("shared", 0);
        assertTrue(queues(storage).isEmpty());
        storage.retainQueue("refreshed", 0, "q", "url"); storage.refresh();
        Field users = S3QS.class.getDeclaredField("queueUsers"); users.setAccessible(true);
        assertTrue(((Map<?, ?>) users.get(storage)).isEmpty());
        verifyNoInteractions(s3, sqs);
    }

    @Test(timeout = 10000)
    public void warmWorkerReleasesEveryQueryAndFailedPartialRegistration() throws Exception
    {
        S3QS storage = new S3QS(mock(S3Client.class), mock(SqsClient.class), 30);
        Field shared = WorkerCommon.class.getDeclaredField("s3qs"); shared.setAccessible(true); Object old = shared.get(null);
        shared.set(null, storage);
        try
        {
            for (int query = 0; query < 100; query++)
            {
                S3QSQueueScope scope = new S3QSQueueScope(); scope.register(info("query" + query, false));
                assertEquals(2, queues(storage).size()); scope.closeAfter(null); assertTrue(queues(storage).isEmpty());
            }
            S3QSQueueScope partial = new S3QSQueueScope();
            assertThrows(IOException.class, () -> partial.register(info("partial", true)));
            assertEquals(1, queues(storage).size()); partial.closeAfter(null); assertTrue(queues(storage).isEmpty());
        }
        finally { shared.set(null, old); }
    }

    @Test(timeout = 10000)
    public void cancellationRetainsQueueUntilSlowChildReallyStops() throws Exception
    {
        S3QS storage = new S3QS(mock(S3Client.class), mock(SqsClient.class), 30);
        Field shared = WorkerCommon.class.getDeclaredField("s3qs"); shared.setAccessible(true); Object old = shared.get(null);
        shared.set(null, storage);
        CountDownLatch entered = new CountDownLatch(1), finish = new CountDownLatch(1);
        WorkerThreadPool pool = new WorkerThreadPool(1, Executors.defaultThreadFactory());
        try
        {
            S3QSQueueScope scope = new S3QSQueueScope(); scope.register(info("slow", false));
            pool.execute(() -> {
                entered.countDown(); boolean stopped = false;
                while (!stopped) { try { finish.await(); stopped = true; } catch (InterruptedException ignored) { } }
            });
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            scope.closeAfter(pool); assertEquals(2, queues(storage).size());
            finish.countDown(); assertTrue(pool.awaitTermination(2, TimeUnit.SECONDS));
            assertTrue(queues(storage).isEmpty());
        }
        finally { finish.countDown(); pool.shutdownNow(); pool.awaitTermination(2, TimeUnit.SECONDS); shared.set(null, old); }
    }

    @Test(timeout = 5000)
    public void queueScopeAlsoWaitsForProducerWritePool() throws Exception
    {
        S3QS storage = new S3QS(mock(S3Client.class), mock(SqsClient.class), 30);
        Field shared = WorkerCommon.class.getDeclaredField("s3qs"); shared.setAccessible(true); Object old = shared.get(null);
        shared.set(null, storage);
        WorkerContext context = new WorkerContext(mock(org.apache.logging.log4j.Logger.class), new WorkerMetrics(), "test");
        ShuffleInfo info = info("writes", false);
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(storage, info, 0, 2, context))
        {
            S3QSQueueScope scope = new S3QSQueueScope(); scope.register(info); scope.own(output);
            scope.closeAfter(null); assertEquals(2, queues(storage).size());
            output.close(); assertTrue(queues(storage).isEmpty());
        }
        finally { shared.set(null, old); }
    }

    @Test(timeout = 10000)
    public void lateAcknowledgementKeepsItsQueueAfterHandlerLeaseIsReleased() throws Exception
    {
        S3QS storage = new S3QS(mock(S3Client.class), mock(SqsClient.class), 30);
        storage.retainQueue("ack", 0, "q", "url");
        CountDownLatch entered = new CountDownLatch(1), finish = new CountDownLatch(1);
        S3QSAsyncTasks tasks = new S3QSAsyncTasks(WorkerCancellation.disabled(), 1, 1);
        S3QSQueueScope scope = new S3QSQueueScope();
        scope.retain(storage, "ack", 0); scope.own(tasks);
        try
        {
            tasks.submit(() -> {
                entered.countDown();
                while (true)
                {
                    try { finish.await(); return; }
                    catch (InterruptedException ignored) { }
                }
            });
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            scope.closeAfter(null); scope.closeAfter(null);
            storage.releaseQueue("ack", 0);
            assertEquals(1, queues(storage).size());
            finish.countDown(); tasks.await(); assertTrue(queues(storage).isEmpty());
        }
        finally { finish.countDown(); tasks.close(); }
    }

    @Test
    public void repeatedCloseDoesNotReleaseAnotherTasksLeaseAndCleanupContinuesAfterFailure() throws Exception
    {
        S3QS storage = new S3QS(mock(S3Client.class), mock(SqsClient.class), 30);
        storage.retainQueue("shared", 0, "q", "url");
        S3QSQueueScope scope = new S3QSQueueScope();
        scope.retain(storage, "shared", 0);
        scope.closeAfter(null); scope.closeAfter(null);
        assertEquals(1, queues(storage).size());
        storage.releaseQueue("shared", 0); assertTrue(queues(storage).isEmpty());
        assertThrows(IllegalStateException.class, () -> scope.retain(storage, "late", 0));

        storage.registerQueue("cleanup", 0, "q0", "url0");
        storage.registerQueue("cleanup", 1, "q1", "url1");
        S3QSQueueScope failing = new S3QSQueueScope();
        failing.retain(storage, "cleanup", 0); failing.retain(storage, "cleanup", 1);
        S3Queue broken = mock(S3Queue.class);
        doThrow(new IOException("injected close failure")).when(broken).close();
        @SuppressWarnings("unchecked") Map<Object, Object> entries = (Map<Object, Object>) queues(storage);
        entries.put(entries.keySet().iterator().next(), broken);
        failing.closeAfter(null); failing.closeAfter(null);
        assertTrue(entries.isEmpty()); verify(broken, times(1)).close();
    }

    private static ShuffleInfo info(String id, boolean invalid)
    {
        return new ShuffleInfo(id, new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                "bucket/" + id, 2, 1, 1, 1, Arrays.asList(
                new ShuffleQueueInfo(0, "q0", "url0"), new ShuffleQueueInfo(invalid ? 3 : 1, "q1", "url1")));
    }

    private static Map<?, ?> queues(S3QS storage) throws Exception
    {
        Field field = S3QS.class.getDeclaredField("partitionQueues"); field.setAccessible(true);
        return (Map<?, ?>) field.get(storage);
    }
}
