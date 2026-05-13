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
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestIngestFilePublisher
{
    @Test
    public void finishClosesPhysicalFileOnlyOnceAndLeavesMetadataTemporary() throws Exception
    {
        CountingPixelsWriter writer = new CountingPixelsWriter();
        File file = temporaryFile(101L);
        FileWriterManager fileWriterManager = testFileWriterManager(writer, file);

        CompletableFuture<Void> firstFinish = fileWriterManager.finish();
        CompletableFuture<Void> secondFinish = fileWriterManager.finish();
        firstFinish.get(5, TimeUnit.SECONDS);
        secondFinish.get(5, TimeUnit.SECONDS);

        assertSame(firstFinish, secondFinish);
        assertEquals(1, writer.closeCount.get());
        assertEquals(File.Type.TEMPORARY, file.getType());
        assertEquals(File.Type.TEMPORARY, fileWriterManager.getFileSnapshot().getType());
        assertTrue(firstFinish.isDone());
        assertFalse(firstFinish.isCompletedExceptionally());
    }

    @Test
    public void finishFailureIsPropagatedAndDoesNotPublishMetadata() throws Exception
    {
        IOException closeFailure = new IOException("close failed");
        CountingPixelsWriter writer = new CountingPixelsWriter(null, null, closeFailure, null);
        File file = temporaryFile(103L);
        FileWriterManager fileWriterManager = testFileWriterManager(writer, file);

        CompletableFuture<Void> firstFinish = fileWriterManager.finish();
        try
        {
            firstFinish.get(5, TimeUnit.SECONDS);
            fail("Expected physical close failure");
        } catch (ExecutionException e)
        {
            assertSame(closeFailure, e.getCause());
        }

        CompletableFuture<Void> secondFinish = fileWriterManager.finish();
        assertSame(firstFinish, secondFinish);
        assertTrue(secondFinish.isCompletedExceptionally());
        assertEquals(1, writer.closeCount.get());
        assertEquals(File.Type.TEMPORARY, file.getType());
        assertEquals(File.Type.TEMPORARY, fileWriterManager.getFileSnapshot().getType());
    }

    @Test
    public void fileSnapshotCopiesCurrentFileMetadata()
    {
        File file = temporaryFile(202L);
        CountingPixelsWriter writer = new CountingPixelsWriter();
        FileWriterManager fileWriterManager = testFileWriterManager(writer, file);

        File snapshot = fileWriterManager.getFileSnapshot();

        assertEquals(file.getId(), snapshot.getId());
        assertEquals(file.getName(), snapshot.getName());
        assertEquals(file.getType(), snapshot.getType());
        assertEquals(file.getNumRowGroup(), snapshot.getNumRowGroup());
        assertEquals(file.getMinRowId(), snapshot.getMinRowId());
        assertEquals(file.getMaxRowId(), snapshot.getMaxRowId());
        assertEquals(file.getPathId(), snapshot.getPathId());
    }

    @Test
    public void fileSnapshotDoesNotExposeInternalFileState()
    {
        File file = temporaryFile(203L);
        CountingPixelsWriter writer = new CountingPixelsWriter();
        FileWriterManager fileWriterManager = testFileWriterManager(writer, file);
        File snapshot = fileWriterManager.getFileSnapshot();

        snapshot.setName("published.pxl");
        snapshot.setType(File.Type.REGULAR);
        snapshot.setNumRowGroup(99);
        snapshot.setMinRowId(1000);
        snapshot.setMaxRowId(2000);
        snapshot.setPathId(88L);

        File freshSnapshot = fileWriterManager.getFileSnapshot();
        assertEquals("ingest_203.pxl", file.getName());
        assertEquals(File.Type.TEMPORARY, file.getType());
        assertEquals(1, file.getNumRowGroup());
        assertEquals(0, file.getMinRowId());
        assertEquals(63, file.getMaxRowId());
        assertEquals(9L, file.getPathId());
        assertEquals(file.getName(), freshSnapshot.getName());
        assertEquals(file.getType(), freshSnapshot.getType());
        assertEquals(file.getNumRowGroup(), freshSnapshot.getNumRowGroup());
        assertEquals(file.getMinRowId(), freshSnapshot.getMinRowId());
        assertEquals(file.getMaxRowId(), freshSnapshot.getMaxRowId());
        assertEquals(file.getPathId(), freshSnapshot.getPathId());
    }

    @Test
    public void fileSnapshotReflectsMutationsOnUnderlyingFile()
    {
        File file = temporaryFile(205L);
        CountingPixelsWriter writer = new CountingPixelsWriter();
        FileWriterManager fileWriterManager = testFileWriterManager(writer, file);

        File before = fileWriterManager.getFileSnapshot();
        assertEquals(File.Type.TEMPORARY, before.getType());
        assertEquals(63L, before.getMaxRowId());

        // Mutations on the underlying file (e.g. visibility/row id updates) must be observed
        // by snapshots taken afterwards. Snapshots taken earlier must remain unchanged.
        file.setMaxRowId(127L);
        file.setNumRowGroup(2);

        File after = fileWriterManager.getFileSnapshot();
        assertEquals(127L, after.getMaxRowId());
        assertEquals(2, after.getNumRowGroup());
        // The previously taken snapshot must keep its original values.
        assertEquals(63L, before.getMaxRowId());
        assertEquals(1, before.getNumRowGroup());
    }

    @Test
    public void gettersExposeConstructorArguments()
    {
        File file = temporaryFile(301L);
        CountingPixelsWriter writer = new CountingPixelsWriter();
        FileWriterManager fileWriterManager = new FileWriterManager(7L, writer, file, 5L, 10L, 0);

        assertEquals(file.getId(), fileWriterManager.getFileId());
        assertEquals(5L, fileWriterManager.getFirstBlockId());
        assertEquals(10L, fileWriterManager.getLastBlockId());
    }

    @Test
    public void setLastBlockIdUpdatesGetter()
    {
        File file = temporaryFile(302L);
        CountingPixelsWriter writer = new CountingPixelsWriter();
        FileWriterManager fileWriterManager = new FileWriterManager(1L, writer, file, 0L, 0L, 0);

        fileWriterManager.setLastBlockId(42L);
        assertEquals(42L, fileWriterManager.getLastBlockId());

        // Allow lowering as well, e.g. when shrinking the range during close().
        fileWriterManager.setLastBlockId(-1L);
        assertEquals(-1L, fileWriterManager.getLastBlockId());
    }

    @Test
    public void addRowBatchSucceedsAndForwardsToWriter() throws Exception
    {
        CountingPixelsWriter writer = new CountingPixelsWriter();
        File file = temporaryFile(401L);
        FileWriterManager fileWriterManager = testFileWriterManager(writer, file);

        fileWriterManager.addRowBatch(null);
        fileWriterManager.addRowBatch(null);
        fileWriterManager.addRowBatch(null);

        assertEquals(3, writer.addRowBatchCount.get());
        assertEquals(0, writer.closeCount.get());
        assertEquals(File.Type.TEMPORARY, file.getType());
    }

    @Test
    public void addRowBatchFailureLeavesManagerUsableForFinish() throws Exception
    {
        IOException writeFailure = new IOException("write failed");
        CountingPixelsWriter writer = new CountingPixelsWriter(null, null, null, writeFailure);
        File file = temporaryFile(402L);
        FileWriterManager fileWriterManager = testFileWriterManager(writer, file);

        try
        {
            fileWriterManager.addRowBatch(null);
            fail("Expected row batch write failure");
        } catch (RetinaException e)
        {
            assertSame(writeFailure, e.getCause());
        }

        // After a failed addRowBatch, finish() must still close the underlying writer exactly once
        // and keep the file in TEMPORARY state (publication is the buffer's responsibility).
        fileWriterManager.finish().get(5, TimeUnit.SECONDS);
        assertEquals(1, writer.closeCount.get());
        assertEquals(File.Type.TEMPORARY, file.getType());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void finishIsIdempotentUnderConcurrentCallers() throws Exception
    {
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        CountingPixelsWriter writer = new CountingPixelsWriter(closeStarted, allowClose, null, null);
        File file = temporaryFile(501L);
        FileWriterManager fileWriterManager = testFileWriterManager(writer, file);

        // Start the first finish() so the close thread is parked inside writer.close().
        CompletableFuture<Void> firstFinish = fileWriterManager.finish();
        assertTrue(closeStarted.await(5, TimeUnit.SECONDS));

        int callerCount = 8;
        ExecutorService callers = Executors.newFixedThreadPool(callerCount);
        try
        {
            CountDownLatch readyLatch = new CountDownLatch(callerCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            Future<CompletableFuture<Void>>[] results = new Future[callerCount];
            for (int i = 0; i < callerCount; ++i)
            {
                results[i] = callers.submit(() -> {
                    readyLatch.countDown();
                    startLatch.await();
                    return fileWriterManager.finish();
                });
            }
            assertTrue(readyLatch.await(5, TimeUnit.SECONDS));
            startLatch.countDown();

            for (Future<CompletableFuture<Void>> result : results)
            {
                CompletableFuture<Void> observed = result.get(5, TimeUnit.SECONDS);
                assertSame(firstFinish, observed);
                assertFalse(observed.isDone());
            }
        } finally
        {
            allowClose.countDown();
            callers.shutdownNow();
        }

        firstFinish.get(5, TimeUnit.SECONDS);
        assertEquals("writer.close() must run at most once even under concurrent finish() calls",
                1, writer.closeCount.get());
        assertEquals(File.Type.TEMPORARY, file.getType());
    }

    @Test
    public void finishRunsCloseOnDedicatedNamedThread() throws Exception
    {
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        ThreadCapturingPixelsWriter writer = new ThreadCapturingPixelsWriter(closeStarted, allowClose);
        File file = temporaryFile(601L);
        FileWriterManager fileWriterManager = new FileWriterManager(1L, writer, file, 1L, 0L, 0);

        Thread caller = Thread.currentThread();
        CompletableFuture<Void> finishFuture = fileWriterManager.finish();
        assertTrue(closeStarted.await(5, TimeUnit.SECONDS));

        Thread closeThread = writer.closeThread;
        assertNotSame("close() must run off the caller thread", caller, closeThread);
        assertEquals("pixels-retina-file-finish-" + file.getId(), closeThread.getName());

        allowClose.countDown();
        finishFuture.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void finishPropagatesRuntimeExceptionFromClose() throws Exception
    {
        RuntimeException closeFailure = new RuntimeException("boom");
        CountingPixelsWriter writer = new CountingPixelsWriter(null, null, null, null,
                closeFailure);
        File file = temporaryFile(701L);
        FileWriterManager fileWriterManager = testFileWriterManager(writer, file);

        CompletableFuture<Void> firstFinish = fileWriterManager.finish();
        try
        {
            firstFinish.get(5, TimeUnit.SECONDS);
            fail("Expected runtime close failure");
        } catch (ExecutionException e)
        {
            assertSame(closeFailure, e.getCause());
        }

        // Subsequent calls must keep returning the same failed future and must not retry close().
        CompletableFuture<Void> secondFinish = fileWriterManager.finish();
        assertSame(firstFinish, secondFinish);
        assertTrue(secondFinish.isCompletedExceptionally());
        assertEquals(1, writer.closeCount.get());
        assertEquals(File.Type.TEMPORARY, file.getType());
    }

    @Test(timeout = 10_000L)
    public void finishDoesNotBlockCallerThread() throws Exception
    {
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        CountingPixelsWriter writer = new CountingPixelsWriter(closeStarted, allowClose, null, null);
        File file = temporaryFile(801L);
        FileWriterManager fileWriterManager = testFileWriterManager(writer, file);

        long start = System.nanoTime();
        CompletableFuture<Void> finishFuture = fileWriterManager.finish();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        // The caller thread must return promptly; the actual close() runs on the named thread.
        assertTrue("finish() must not block on writer.close(); elapsedMillis=" + elapsedMillis,
                elapsedMillis < 2_000L);
        assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
        assertFalse(finishFuture.isDone());
        try
        {
            finishFuture.get(200, TimeUnit.MILLISECONDS);
            fail("finish() future must not complete before writer.close() returns");
        } catch (TimeoutException expected)
        {
            // expected: still in progress
        }
        allowClose.countDown();
        finishFuture.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void concurrentAddRowBatchesAreAllForwardedToWriter() throws Exception
    {
        // FileWriterManager does not perform internal locking around addRowBatch; verify it does
        // not lose calls or throw NPEs when several threads forward row batches concurrently.
        CountingPixelsWriter writer = new CountingPixelsWriter();
        File file = temporaryFile(1601L);
        FileWriterManager fileWriterManager = testFileWriterManager(writer, file);

        int callerCount = 16;
        int callsPerCaller = 25;
        ExecutorService callers = Executors.newFixedThreadPool(callerCount);
        try
        {
            CountDownLatch startLatch = new CountDownLatch(1);
            List<Future<Void>> results = new ArrayList<>(callerCount);
            for (int i = 0; i < callerCount; ++i)
            {
                results.add(callers.submit(() -> {
                    startLatch.await();
                    for (int j = 0; j < callsPerCaller; ++j)
                    {
                        fileWriterManager.addRowBatch(null);
                    }
                    return null;
                }));
            }
            startLatch.countDown();
            for (Future<Void> result : results)
            {
                result.get(10, TimeUnit.SECONDS);
            }
        } finally
        {
            callers.shutdownNow();
        }

        assertEquals(callerCount * callsPerCaller, writer.addRowBatchCount.get());
        assertEquals(0, writer.closeCount.get());
        assertEquals(File.Type.TEMPORARY, file.getType());
    }

    @Test
    public void finishIsRobustAgainstFileMetadataMutationsBeforeReturn() throws Exception
    {
        // Mutations on the underlying file (e.g. visibility/row id updates by other components)
        // performed while finish() is in progress must not affect the success of physical close,
        // and the post-close snapshot must reflect the mutated state because publication has
        // not yet rewritten file.type.
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        CountingPixelsWriter writer = new CountingPixelsWriter(closeStarted, allowClose, null, null);
        File file = temporaryFile(2001L);
        FileWriterManager fileWriterManager = testFileWriterManager(writer, file);

        CompletableFuture<Void> finishFuture = fileWriterManager.finish();
        assertTrue(closeStarted.await(5, TimeUnit.SECONDS));

        // Concurrently update row id bookkeeping; this is what the visibility layer does.
        file.setMaxRowId(255L);
        file.setNumRowGroup(3);

        allowClose.countDown();
        finishFuture.get(5, TimeUnit.SECONDS);

        File snapshot = fileWriterManager.getFileSnapshot();
        assertEquals(255L, snapshot.getMaxRowId());
        assertEquals(3, snapshot.getNumRowGroup());
        assertEquals(File.Type.TEMPORARY, snapshot.getType());
        assertEquals(1, writer.closeCount.get());
    }

    @Test
    public void addRowBatchPropagatesWriterRuntimeExceptionWithoutWrapping() throws Exception
    {
        // FileWriterManager only wraps IOException into RetinaException; unchecked exceptions
        // (e.g. format-corruption indicators thrown by the underlying writer as RuntimeException)
        // must propagate to the caller as-is so they are not silently masked. After such a failure
        // the manager must remain usable and finish() must still close the writer exactly once.
        RuntimeException formatFailure = new IllegalStateException("corrupted column vector");
        CountingPixelsWriter writer = new CountingPixelsWriter()
        {
            @Override
            public boolean addRowBatch(VectorizedRowBatch rowBatch) throws IOException
            {
                addRowBatchCount.incrementAndGet();
                throw formatFailure;
            }
        };
        File file = temporaryFile(2101L);
        FileWriterManager fileWriterManager = testFileWriterManager(writer, file);

        try
        {
            fileWriterManager.addRowBatch(null);
            fail("Runtime exception from writer must propagate without being wrapped");
        } catch (RetinaException e)
        {
            fail("Runtime exception must not be wrapped as RetinaException, got: " + e);
        } catch (IllegalStateException expected)
        {
            assertSame(formatFailure, expected);
        }
        assertEquals(1, writer.addRowBatchCount.get());

        // After a runtime failure inside the writer, finish() must still be able to close it.
        fileWriterManager.finish().get(5, TimeUnit.SECONDS);
        assertEquals(1, writer.closeCount.get());
        assertEquals(File.Type.TEMPORARY, file.getType());
    }

    private static File temporaryFile(long id)
    {
        File file = new File();
        file.setId(id);
        file.setName("ingest_" + id + ".pxl");
        file.setType(File.Type.TEMPORARY);
        file.setNumRowGroup(1);
        file.setMinRowId(0);
        file.setMaxRowId(63);
        file.setPathId(9L);
        return file;
    }

    private static FileWriterManager testFileWriterManager(CountingPixelsWriter writer, File file)
    {
        return new FileWriterManager(1L, writer, file, 1L, 0L, 0);
    }

    private static class CountingPixelsWriter implements PixelsWriter
    {
        // Package-private so anonymous subclasses defined inside this test can observe call counts.
        final AtomicInteger closeCount = new AtomicInteger(0);
        final AtomicInteger addRowBatchCount = new AtomicInteger(0);
        private final CountDownLatch closeStarted;
        private final CountDownLatch allowClose;
        private final IOException closeFailure;
        private final IOException addRowBatchFailure;
        private final RuntimeException closeRuntimeFailure;

        private CountingPixelsWriter()
        {
            this(null, null, null, null, null);
        }

        private CountingPixelsWriter(CountDownLatch closeStarted, CountDownLatch allowClose,
                                     IOException closeFailure, IOException addRowBatchFailure)
        {
            this(closeStarted, allowClose, closeFailure, addRowBatchFailure, null);
        }

        private CountingPixelsWriter(CountDownLatch closeStarted, CountDownLatch allowClose,
                                     IOException closeFailure, IOException addRowBatchFailure,
                                     RuntimeException closeRuntimeFailure)
        {
            this.closeStarted = closeStarted;
            this.allowClose = allowClose;
            this.closeFailure = closeFailure;
            this.addRowBatchFailure = addRowBatchFailure;
            this.closeRuntimeFailure = closeRuntimeFailure;
        }

        @Override
        public boolean addRowBatch(VectorizedRowBatch rowBatch) throws IOException
        {
            addRowBatchCount.incrementAndGet();
            if (addRowBatchFailure != null)
            {
                throw addRowBatchFailure;
            }
            return true;
        }

        @Override
        public void addRowBatch(VectorizedRowBatch rowBatch, int hashValue) throws IOException
        {
        }

        @Override
        public TypeDescription getSchema()
        {
            return null;
        }

        @Override
        public int getNumRowGroup()
        {
            return 0;
        }

        @Override
        public int getNumWriteRequests()
        {
            return 0;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public void close() throws IOException
        {
            closeCount.incrementAndGet();
            if (closeStarted != null)
            {
                closeStarted.countDown();
            }
            if (allowClose != null)
            {
                try
                {
                    assertTrue(allowClose.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting to close", e);
                }
            }
            if (closeFailure != null)
            {
                throw closeFailure;
            }
            if (closeRuntimeFailure != null)
            {
                throw closeRuntimeFailure;
            }
        }
    }

    private static class ThreadCapturingPixelsWriter implements PixelsWriter
    {
        private final CountDownLatch closeStarted;
        private final CountDownLatch allowClose;
        private volatile Thread closeThread;

        private ThreadCapturingPixelsWriter(CountDownLatch closeStarted, CountDownLatch allowClose)
        {
            this.closeStarted = closeStarted;
            this.allowClose = allowClose;
        }

        @Override
        public boolean addRowBatch(VectorizedRowBatch rowBatch)
        {
            return true;
        }

        @Override
        public void addRowBatch(VectorizedRowBatch rowBatch, int hashValue)
        {
        }

        @Override
        public TypeDescription getSchema()
        {
            return null;
        }

        @Override
        public int getNumRowGroup()
        {
            return 0;
        }

        @Override
        public int getNumWriteRequests()
        {
            return 0;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public void close() throws IOException
        {
            this.closeThread = Thread.currentThread();
            if (closeStarted != null)
            {
                closeStarted.countDown();
            }
            if (allowClose != null)
            {
                try
                {
                    assertTrue(allowClose.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting to close", e);
                }
            }
        }
    }
}
