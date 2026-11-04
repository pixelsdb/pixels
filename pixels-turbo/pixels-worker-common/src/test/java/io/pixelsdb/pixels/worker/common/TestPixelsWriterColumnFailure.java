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

import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.core.writer.ColumnWriter;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestPixelsWriterColumnFailure
{
    private static final TypeDescription SCHEMA = TypeDescription.fromString("struct<a:bigint,b:bigint>");

    private static PixelsWriterImpl writer(PhysicalWriter physical, boolean hidden) throws Exception
    {
        return writer(physical, hidden, null);
    }

    private static PixelsWriterImpl writer(PhysicalWriter physical, boolean hidden, ExecutorService pool) throws Exception
    {
        PixelsWriterImpl.Builder builder = PixelsWriterImpl.newBuilder().setStorage(mock(Storage.class)).setPath("column-test")
                .setPhysicalWriter(physical).setSchema(SCHEMA).setHasHiddenColumn(hidden)
                .setPixelStride(1024).setRowGroupSize(1024 * 1024);
        if (pool != null) { builder.setColumnWriterService(pool); }
        return (PixelsWriterImpl) builder.build();
    }

    @Test(timeout = 5000)
    public void borrowedPoolSurvivesFileCloseAbortAndEncodingFailure() throws Exception
    {
        ExecutorService pool = Executors.newCachedThreadPool();
        try
        {
            writer(mock(PhysicalWriter.class), false, pool).close();
            assertFalse(pool.isShutdown());
            writer(mock(PhysicalWriter.class), true, pool).abort();
            assertFalse(pool.isShutdown());
            PixelsWriterImpl failed = writer(mock(PhysicalWriter.class), false, pool);
            try
            {
                when(replaceColumns(failed)[0].write(any(ColumnVector.class), anyInt()))
                        .thenThrow(new IOException("injected borrowed-pool encoding failure"));
                VectorizedRowBatch batch = SCHEMA.createRowBatch(1); batch.size = 1;
                assertThrows(CompletionException.class, () -> failed.addRowBatch(batch));
            }
            finally { failed.abort(); }
            assertFalse(pool.isShutdown());
            assertEquals(42, (int) pool.submit(() -> 42).get(1, TimeUnit.SECONDS));
        }
        finally { pool.shutdownNow(); assertTrue(pool.awaitTermination(1, TimeUnit.SECONDS)); }
    }

    private static Field field(String name) throws Exception
    {
        Field field = PixelsWriterImpl.class.getDeclaredField(name); field.setAccessible(true); return field;
    }

    private static ColumnWriter[] replaceColumns(PixelsWriterImpl writer) throws Exception
    {
        ColumnWriter[] columns = (ColumnWriter[]) field("columnWriters").get(writer);
        for (int i = 0; i < columns.length; i++) { columns[i].close(); columns[i] = mock(ColumnWriter.class); }
        return columns;
    }

    @Test(timeout = 5000)
    public void checkedUncheckedAndHiddenColumnFailuresCompleteWaitingCaller() throws Exception
    {
        for (boolean hidden : new boolean[]{false, true})
        {
            for (boolean checked : new boolean[]{false, true})
            {
                PixelsWriterImpl writer = writer(mock(PhysicalWriter.class), hidden);
                try
                {
                    ColumnWriter[] columns = replaceColumns(writer);
                    ColumnWriter failed = columns[0];
                    if (hidden)
                    {
                        ((ColumnWriter) field("hiddenColumnWriter").get(writer)).close();
                        failed = mock(ColumnWriter.class); field("hiddenColumnWriter").set(writer, failed);
                    }
                    Throwable cause = checked ? new IOException("injected encoding error") : new ClassCastException("injected type error");
                    when(failed.write(any(ColumnVector.class), anyInt())).thenThrow(cause);
                    VectorizedRowBatch batch = hidden ? SCHEMA.createRowBatchWithHiddenColumn() : SCHEMA.createRowBatch(1);
                    batch.size = 1;
                    CompletionException failure = assertThrows(CompletionException.class, () -> writer.addRowBatch(batch));
                    assertSame(cause, failure.getCause());
                }
                finally { writer.abort(); }
            }
        }
    }

    @Test(timeout = 5000)
    public void columnsRemainParallelOnThePerFilePool() throws Exception
    {
        checkParallelColumns(null);
    }

    @Test(timeout = 5000)
    public void columnsRemainParallelOnABorrowedPool() throws Exception
    {
        ExecutorService pool = Executors.newCachedThreadPool();
        try { checkParallelColumns(pool); }
        finally { pool.shutdownNow(); assertTrue(pool.awaitTermination(1, TimeUnit.SECONDS)); }
    }

    private static void checkParallelColumns(ExecutorService pool) throws Exception
    {
        PixelsWriterImpl writer = writer(mock(PhysicalWriter.class), false, pool);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(2), release = new CountDownLatch(1);
        try
        {
            for (ColumnWriter column : replaceColumns(writer))
            {
                when(column.write(any(ColumnVector.class), anyInt())).thenAnswer(call -> {
                    entered.countDown(); assertTrue(release.await(2, TimeUnit.SECONDS)); return 0;
                });
            }
            VectorizedRowBatch batch = SCHEMA.createRowBatch(1); batch.size = 1;
            Future<?> writing = caller.submit(() -> { writer.addRowBatch(batch); return null; });
            assertTrue("both column tasks must enter before either completes", entered.await(2, TimeUnit.SECONDS));
            release.countDown(); writing.get(1, TimeUnit.SECONDS);
        }
        finally { release.countDown(); caller.shutdownNow(); writer.abort(); }
    }

    @Test(timeout = 5000)
    public void rejectedSubmissionStillWaitsForAlreadyRunningColumns() throws Exception
    {
        PixelsWriterImpl writer = writer(mock(PhysicalWriter.class), false);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger submissions = new AtomicInteger();
        ExecutorService pool = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>())
        {
            @Override public void execute(Runnable task)
            {
                if (submissions.incrementAndGet() > 1) { throw new RejectedExecutionException("injected rejection"); }
                super.execute(task);
            }
        };
        ((ExecutorService) field("columnWriterService").get(writer)).shutdownNow();
        field("columnWriterService").set(writer, pool);
        try
        {
            ColumnWriter[] columns = replaceColumns(writer);
            when(columns[0].write(any(ColumnVector.class), anyInt())).thenAnswer(call -> {
                entered.countDown(); release.await(); return 0;
            });
            VectorizedRowBatch batch = SCHEMA.createRowBatch(1); batch.size = 1;
            Future<?> writing = caller.submit(() -> { writer.addRowBatch(batch); return null; });
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> writing.get(100, TimeUnit.MILLISECONDS));
            release.countDown();
            ExecutionException failure = assertThrows(ExecutionException.class, () -> writing.get(1, TimeUnit.SECONDS));
            assertTrue(failure.getCause().getCause() instanceof RejectedExecutionException);
        }
        finally { release.countDown(); caller.shutdownNow(); writer.abort(); pool.shutdownNow(); }
    }

    @Test(timeout = 5000)
    public void physicalCloseFailureStillShutsDownColumnPool() throws Exception
    {
        PhysicalWriter physical = mock(PhysicalWriter.class);
        PixelsWriterImpl writer = writer(physical, false);
        ExecutorService pool = (ExecutorService) field("columnWriterService").get(writer);
        doThrow(new IOException("injected close error")).when(physical).close();
        writer.close();
        assertTrue(pool.isShutdown());
        assertTrue(pool.awaitTermination(1, TimeUnit.SECONDS));
    }
}
