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

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.FixSizedBuffers;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.vector.*;
import io.pixelsdb.pixels.executor.join.Partitioner;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import org.junit.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestPartitionMemory
{
    @Test(timeout = 5000)
    public void byteBudgetBackpressuresUntilRealReleaseAndRejectsOversize() throws Exception
    {
        PartitionByteBudget budget = new PartitionByteBudget(100);
        budget.acquire(80, () -> {});
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try
        {
            Future<?> second = caller.submit(() -> budget.acquire(30, () -> {}));
            assertThrows(TimeoutException.class, () -> second.get(100, TimeUnit.MILLISECONDS));
            budget.release(80);
            second.get(1, TimeUnit.SECONDS);
            assertEquals(80, budget.peak());
            assertTrue(budget.waitNs() > 0);
            budget.release(30);
            assertEquals(0, budget.used());
            assertThrows(WorkerException.class, () -> budget.acquire(101, () -> {}));
            assertThrows(IllegalStateException.class, () -> budget.release(1));
        }
        finally { caller.shutdownNow(); }
    }

    @Test(timeout = 5000)
    public void cancellationReleasesQueuedWriteReservationsExactlyOnce() throws Exception
    {
        WorkerCancellation cancellation = new WorkerCancellation();
        PartitionByteBudget budget = new PartitionByteBudget(1200);
        CountDownLatch started = new CountDownLatch(1);
        AtomicInteger finished = new AtomicInteger();
        try (S3QSAsyncTasks tasks = new S3QSAsyncTasks(cancellation, 1))
        {
            for (int i = 0; i < 12; i++)
            {
                budget.acquire(100, cancellation::check);
                tasks.submit(() -> { started.countDown(); new CountDownLatch(1).await(); }, () ->
                {
                    budget.release(100); finished.incrementAndGet();
                });
            }
            assertTrue(started.await(1, TimeUnit.SECONDS));
            cancellation.cancel(new RuntimeException("cancel pending IO"));
        }
        assertEquals(0, budget.used());
        assertEquals(12, finished.get());
        assertTrue(cancellation.isQuiescent());
    }

    @Test
    public void rejectedSubmissionAlsoReturnsItsBytes()
    {
        S3QSAsyncTasks tasks = new S3QSAsyncTasks(new WorkerCancellation(), 1);
        tasks.close();
        AtomicInteger releases = new AtomicInteger();
        assertThrows(WorkerException.class, () -> tasks.submit(() -> fail("must not run"), releases::incrementAndGet));
        assertEquals(1, releases.get());
    }

    @Test
    public void ordinaryWindowsUseOnlyFileTailWithoutFetchingAnyRowGroupFooters() throws Exception
    {
        PixelsReader reader = mock(PixelsReader.class);
        when(reader.getRowGroupInfo(anyInt())).thenReturn(PixelsProto.RowGroupInformation.newBuilder().setDataLength(40 * 10240).build());
        when(reader.getFileSchema()).thenReturn(TypeDescription.fromString("struct<k:bigint>"));
        PartitionMemoryPolicy policy = new PartitionMemoryPolicy(2048 * 10240, 400 * 10240, 400 * 10240, 2, 4);
        InputInfo original = new InputInfo("file", 3, 7);
        PartitionReadWindow window = PartitionReadWindow.next(reader, original, 3, 10, new String[]{"k"}, policy, 400 * 10240);
        assertEquals(2, window.input.getRgLength());
        assertEquals(160 * 10240 + window.decodedBytes, window.bytes);
        assertTrue(window.decodedBytes > 0);
        assertFalse(window.exactSizing);
        assertEquals(7, original.getRgLength());
        verify(reader, never()).getRowGroupFooter(anyInt());
        verify(reader).getFileSchema();
        verify(reader, never()).getRowGroupInfo(6);
    }

    @Test
    public void oversizedWholeGroupFallsBackOnlyForCurrentGroupAndSelectedColumns() throws Exception
    {
        PixelsReader reader = mock(PixelsReader.class);
        when(reader.getRowGroupInfo(anyInt())).thenReturn(PixelsProto.RowGroupInformation.newBuilder().setDataLength(1040 * 10240).build());
        when(reader.getFileSchema()).thenReturn(TypeDescription.fromString("struct<k:bigint,ignored:bigint>"));
        PixelsProto.ColumnChunkIndex k = PixelsProto.ColumnChunkIndex.newBuilder().setChunkLength(40 * 10240).build();
        PixelsProto.ColumnChunkIndex other = PixelsProto.ColumnChunkIndex.newBuilder().setChunkLength(1000 * 10240).build();
        when(reader.getRowGroupFooter(anyInt())).thenReturn(PixelsProto.RowGroupFooter.newBuilder()
                .setRowGroupIndexEntry(PixelsProto.RowGroupIndex.newBuilder()
                        .addColumnChunkIndexEntries(k).addColumnChunkIndexEntries(other)).build());
        PartitionMemoryPolicy policy = new PartitionMemoryPolicy(1024 * 10240, 200 * 10240, 200 * 10240, 2, 4);
        InputInfo original = new InputInfo("file", 3, 7);
        PartitionReadWindow window = PartitionReadWindow.next(reader, original, 3, 10, new String[]{"k"}, policy, 200 * 10240);
        assertEquals(1, window.input.getRgLength());
        assertEquals(80 * 10240 + window.decodedBytes, window.bytes);
        assertTrue(window.exactSizing);
        assertEquals(7, original.getRgLength());
        verify(reader).getRowGroupFooter(3);
        verify(reader, never()).getRowGroupFooter(4);
        assertThrows(java.io.IOException.class, () -> PartitionReadWindow.next(reader, original, 3, 10,
                new String[]{"k", "ignored"}, policy, 200 * 10240));
    }

    @Test
    public void missingLengthsUseBoundedFallbackAndUnsignedLengthsCannotBypassBudget() throws Exception
    {
        PixelsReader reader = mock(PixelsReader.class);
        when(reader.getFileSchema()).thenReturn(TypeDescription.fromString("struct<k:bigint>"));
        when(reader.getRowGroupInfo(0)).thenReturn(PixelsProto.RowGroupInformation.getDefaultInstance());
        when(reader.getRowGroupInfo(1)).thenReturn(PixelsProto.RowGroupInformation.newBuilder().setDataLength(-1).build());
        when(reader.getRowGroupFooter(anyInt())).thenReturn(PixelsProto.RowGroupFooter.newBuilder()
                .setRowGroupIndexEntry(PixelsProto.RowGroupIndex.newBuilder().addColumnChunkIndexEntries(
                        PixelsProto.ColumnChunkIndex.newBuilder().setChunkLength(40 * 10240))).build());
        PartitionMemoryPolicy policy = new PartitionMemoryPolicy(1024 * 10240, 200 * 10240, 200 * 10240, 1, 4);
        InputInfo original = new InputInfo("file", 0, 10000);
        assertEquals(1, PartitionReadWindow.next(reader, original, 0, 10000, new String[]{"k"}, policy, 200 * 10240).input.getRgLength());
        assertEquals(80 * 10240, PartitionReadWindow.next(reader, original, 1, 10000, new String[]{"k"}, policy, 200 * 10240).encodedBytes * 2);
        verify(reader).getRowGroupFooter(0);
        verify(reader).getRowGroupFooter(1);
        verify(reader, never()).getRowGroupInfo(2);
    }

    @Test
    public void ownedBinaryPartitionTailsDoNotKeepInputArrays()
    {
        TypeDescription schema = TypeDescription.fromString("struct<k:bigint,s:string>");
        VectorizedRowBatch input = schema.createRowBatch(4);
        ((LongColumnVector) input.cols[0]).vector[0] = 1;
        byte[] borrowed = new byte[]{'a','b','c'};
        ((BinaryColumnVector) input.cols[1]).setRef(0, borrowed, 0, borrowed.length);
        input.size = 1;
        Partitioner partitioner = new Partitioner(1, 4, schema, new int[]{0}, true);
        partitioner.partition(input);
        borrowed[0] = 'x';
        BinaryColumnVector tail = (BinaryColumnVector) partitioner.getRowBatches()[0].cols[1];
        assertNotSame(borrowed, tail.vector[0]);
        assertEquals('a', tail.vector[0][tail.start[0]]);
    }

    @Test
    public void dictionaryExpansionIsReservedBeforeReadingCompressedData() throws Exception
    {
        PixelsReader reader = mock(PixelsReader.class);
        when(reader.getFileSchema()).thenReturn(TypeDescription.fromString("struct<s:string>"));
        when(reader.getRowGroupInfo(0)).thenReturn(PixelsProto.RowGroupInformation.newBuilder()
                .setDataLength(64).setNumberOfRows(1000000).build());
        when(reader.getRowGroupFooter(0)).thenReturn(PixelsProto.RowGroupFooter.newBuilder()
                .setRowGroupEncoding(PixelsProto.RowGroupEncoding.newBuilder().addColumnChunkEncodings(
                        PixelsProto.ColumnEncoding.newBuilder().setKind(PixelsProto.ColumnEncoding.Kind.DICTIONARY)
                                .setDictionarySize(100000))).build());
        PartitionMemoryPolicy policy = PartitionMemoryPolicy.forResources(4L << 20, 1);
        InputInfo input = new InputInfo("compressed", 0, 1);
        PartitionReadWindow window = PartitionReadWindow.next(reader, input, 0, 1, new String[]{"s"}, policy, 1L << 20);
        assertEquals(64, window.encodedBytes);
        assertTrue(window.decodedBytes >= 400004);
        assertThrows(java.io.IOException.class, () -> PartitionReadWindow.next(reader, input, 0, 1,
                new String[]{"s"}, policy, 256L << 10));
    }

    @Test
    public void boundedIdlePoolCannotRetainUnboundedBursts()
    {
        FixSizedBuffers pool = new FixSizedBuffers(1024, 2);
        byte[][] burst = new byte[20][];
        for (int i = 0; i < burst.length; i++) { burst[i] = pool.allocate(); }
        for (byte[] bytes : burst) { pool.free(bytes); }
        assertEquals(2, pool.getCachedBufferCount());
        assertEquals(1024, pool.allocate().length);
        assertEquals(1, pool.getCachedBufferCount());
        pool.clear(); assertEquals(0, pool.getCachedBufferCount());
    }

    @Test
    public void policyRoundTripPreservesProducerIdentityAndBudgets()
    {
        PartitionInput input = new PartitionInput(); input.setProducerTaskId(7);
        input.setMemoryPolicy(new PartitionMemoryPolicy(1024, 200, 200, 2, 4));
        PartitionInput decoded = JSON.parseObject(JSON.toJSONString(input), PartitionInput.class);
        decoded.getMemoryPolicy().validate();
        assertEquals(7, decoded.getProducerTaskId());
        assertEquals(200, decoded.getMemoryPolicy().getInputBytes());
        assertThrows(IllegalArgumentException.class, () -> new PartitionMemoryPolicy(1024, 400, 400, 2, 4));
    }
}
