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

import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.planner.plan.physical.PartitionTaskPlanner;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestPartitionResources
{
    private static final long MiB = 1L << 20;

    @Test
    public void planningMemoryCapOnlyLowersBackendMemoryAndPreservesCpu()
    {
        ConfigFactory config = ConfigFactory.Instance();
        String key = "executor.partition.memory.planning.mb", previous = config.getProperty(key);
        Invoker backend = mock(Invoker.class);
        when(backend.getMemoryMB()).thenReturn(10240); when(backend.getCpuCount()).thenReturn(6);
        try
        {
            for (String value : new String[]{"0", "6144", "16384", "9223372036854775807"})
            {
                config.addProperty(key, value);
                PartitionMemoryPolicy policy = PartitionMemoryPolicy.fromBackend(backend);
                assertEquals((value.equals("6144") ? 6144 : 10240) * MiB, policy.getWorkerBytes());
                assertEquals(6, policy.getReaderThreads());
                assertEquals(10240, backend.getMemoryMB());
            }
            config.addProperty(key, "6144");
            when(backend.getMemoryMB()).thenReturn(2048);
            assertEquals(2048 * MiB, PartitionMemoryPolicy.fromBackend(backend).getWorkerBytes());
            when(backend.getMemoryMB()).thenReturn(0);
            assertThrows(IllegalArgumentException.class, () -> PartitionMemoryPolicy.fromBackend(backend));
        }
        finally { config.addProperty(key, previous == null ? "0" : previous); }
    }

    @Test
    public void invalidPlanningMemoryCapFailsExplicitly()
    {
        ConfigFactory config = ConfigFactory.Instance();
        String key = "executor.partition.memory.planning.mb", previous = config.getProperty(key);
        Invoker backend = mock(Invoker.class); when(backend.getMemoryMB()).thenReturn(10240);
        try
        {
            for (String invalid : new String[]{"-1", "6gb", "", "1.5", "9223372036854775808"})
            {
                config.addProperty(key, invalid);
                IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                        () -> PartitionMemoryPolicy.fromBackend(backend));
                assertTrue(failure.getMessage().contains(key));
            }
        }
        finally { config.addProperty(key, previous == null ? "0" : previous); }
    }

    @Test
    public void policyUsesBackendMemoryAndCpuAndRejectsUnknownMemory()
    {
        Invoker backend = mock(Invoker.class);
        when(backend.getMemoryMB()).thenReturn(10240); when(backend.getCpuCount()).thenReturn(6);
        PartitionMemoryPolicy policy = PartitionMemoryPolicy.fromBackend(backend);
        assertEquals(10240 * MiB, policy.getWorkerBytes());
        assertEquals(6, policy.getReaderThreads()); assertEquals(0, policy.getWindowRowGroups());
        when(backend.getMemoryMB()).thenReturn(4096); when(backend.getCpuCount()).thenReturn(2);
        PartitionMemoryPolicy small = PartitionMemoryPolicy.fromBackend(backend);
        assertEquals(4096 * MiB, small.getWorkerBytes()); assertEquals(2, small.getReaderThreads());
        assertTrue(small.getInputBytes() < policy.getInputBytes());
        when(backend.getMemoryMB()).thenReturn(0);
        assertThrows(IllegalArgumentException.class, () -> PartitionMemoryPolicy.fromBackend(backend));
    }

    @Test
    public void largerBackendPacksLargerTasksAndCpuChangesSlicesWithoutLosingRanges()
    {
        List<InputSplit> original = ranges(10000, 32);
        List<List<InputSplit>> small = plan(original, 2048, 2, true);
        List<List<InputSplit>> large = plan(original, 10240, 6, true);
        assertTrue(small.size() > large.size());
        assertTrue(large.get(0).size() > 8); // old cap is deliberately removed
        verifyRanges(small, 10000); verifyRanges(large, 10000);
        assertEquals(32, original.get(0).getInputInfos().get(0).getRgLength());
        List<List<InputSplit>> twoCpu = plan(original, 10240, 2, true);
        assertEquals(large.size(), twoCpu.size());
        assertTrue(large.get(0).size() > twoCpu.get(0).size());
    }

    @Test
    public void batchUsesResourceBudgetAndStaysSmallerThanStreaming()
    {
        List<InputSplit> inputs = ranges(1000, 32);
        List<List<InputSplit>> small = plan(inputs, 128, 1, false);
        List<List<InputSplit>> large = plan(inputs, 512, 4, false);
        List<List<InputSplit>> streaming = plan(inputs, 512, 4, true);
        assertTrue(small.size() > large.size()); assertTrue(large.size() > streaming.size());
        verifyRanges(small, 1000); verifyRanges(large, 1000); verifyRanges(streaming, 1000);
    }

    @Test
    public void byteSizedWindowCanReadMoreThanFourGroupsWithoutFooterIO() throws Exception
    {
        PixelsReader reader = mock(PixelsReader.class);
        when(reader.getRowGroupInfo(anyInt())).thenReturn(PixelsProto.RowGroupInformation.newBuilder().setDataLength((int) MiB).build());
        PartitionMemoryPolicy policy = PartitionMemoryPolicy.forResources(1024 * MiB, 4);
        when(reader.getFileSchema()).thenReturn(io.pixelsdb.pixels.core.TypeDescription.fromString("struct<k:bigint>"));
        PartitionReadWindow window = PartitionReadWindow.next(reader, new InputInfo("f", 0, 100), 0, 100,
                new String[]{"k"}, policy, policy.getInputBytes());
        assertTrue(window.input.getRgLength() > 4);
        assertTrue(window.bytes <= policy.getInputBytes() / policy.getReaderThreads());
        verify(reader, never()).getRowGroupFooter(anyInt());
    }

    @Test
    public void runtimeClampsHeapCpuAndSlicesAndPreservesActualPlatformLimit()
    {
        PartitionMemoryPolicy policy = PartitionMemoryPolicy.forResources(10240 * MiB, 6);
        PartitionMemoryAccount account = new PartitionMemoryAccount(policy, 3);
        assertEquals(Math.min(3, Runtime.getRuntime().availableProcessors()), account.policy.getReaderThreads());
        assertTrue(account.input.limit() <= Runtime.getRuntime().maxMemory() * 35 / 100);
        assertTrue(account.input.limit() <= policy.getInputBytes());
        WorkerContext context = new WorkerContext(null, new WorkerMetrics(), "test").withMemoryMB(6144)
                .withCancellation(new WorkerCancellation());
        assertEquals(6144 * MiB, context.getMemoryBytes());
    }

    @Test
    public void unknownFileRangesStayIntactInsteadOfBeingInspectedOrInvented()
    {
        InputInfo unknown = new InputInfo("unknown", 0, -1);
        List<InputSplit> inputs = new ArrayList<>(ranges(30, 10));
        inputs.add(new InputSplit(Collections.singletonList(unknown)));
        List<List<InputSplit>> tasks = plan(inputs, 512, 2, true);
        InputInfo last = tasks.get(tasks.size() - 1).get(0).getInputInfos().get(0);
        assertEquals("unknown", last.getPath()); assertEquals(-1, last.getRgLength());
        assertNotSame(unknown, last);
    }

    @Test
    public void compressedTasksUseDecodedRowsAndUnknownStatisticsUseExpansionFallback()
    {
        Column k = new Column(); k.setName("k"); k.setType("bigint"); k.setSize(100000); k.setChunkSize(100);
        PartitionMemoryPolicy policy = PartitionMemoryPolicy.forResources(128 * MiB, 6);
        List<InputSplit> inputs = ranges(1000, 50);
        List<List<InputSplit>> encodedLike = PartitionTaskPlanner.plan(inputs, Collections.singletonList(k), 1000,
                new String[]{"k"}, new boolean[]{true}, 1024, policy, true, 2, 100);
        List<List<InputSplit>> compressed = PartitionTaskPlanner.plan(inputs, Collections.singletonList(k), 100000000,
                new String[]{"k"}, new boolean[]{true}, 1024, policy, true, 2, 100);
        assertTrue("same encoded bytes, more decoded rows must shorten tasks", compressed.size() > encodedLike.size());
        verifyRanges(compressed, 1000);
        ConfigFactory config = ConfigFactory.Instance();
        String key = "executor.partition.memory.expansion.factor", previous = config.getProperty(key);
        try
        {
            k.setChunkSize(MiB); k.setSize(1000D * MiB);
            config.addProperty(key, "2");
            List<List<InputSplit>> two = PartitionTaskPlanner.plan(inputs, Collections.singletonList(k), 0,
                    new String[]{"k"}, new boolean[]{true}, 1024, policy, true, 2, MiB);
            config.addProperty(key, "8");
            List<List<InputSplit>> eight = PartitionTaskPlanner.plan(inputs, Collections.singletonList(k), 0,
                    new String[]{"k"}, new boolean[]{true}, 1024, policy, true, 2, MiB);
            assertTrue(eight.size() > two.size()); verifyRanges(eight, 1000);
            for (String invalid : new String[]{"0", "NaN", "Infinity", "abc"})
            {
                config.addProperty(key, invalid);
                assertThrows(IllegalArgumentException.class, io.pixelsdb.pixels.planner.plan.physical.PartitionInputSizing::expansionFactor);
            }
        }
        finally { config.addProperty(key, previous == null ? "4" : previous); }
    }

    @Test
    public void totalEnvelopeIsHalfUsableMemoryAndHeapClampKeepsTheRequestedRatio()
    {
        PartitionMemoryPolicy policy = PartitionMemoryPolicy.forResources(10240 * MiB, 6);
        long usable = PartitionMemoryPolicy.usableBytes(policy.getWorkerBytes(), Long.MAX_VALUE);
        assertEquals(usable / 2, policy.getInputBytes() + policy.getOutputBytes());
        assertEquals(4.0, policy.getOutputBytes() / (double) policy.getInputBytes(), 0.0001);
        PartitionMemoryAccount account = new PartitionMemoryAccount(policy, 6);
        long runtimeUsable = PartitionMemoryPolicy.usableBytes(policy.getWorkerBytes(), Runtime.getRuntime().maxMemory());
        assertTrue(account.input.limit() + account.output.limit() <= runtimeUsable / 2);
        assertEquals(4.0, account.output.limit() / (double) account.input.limit(), 0.0001);
        io.pixelsdb.pixels.common.metadata.domain.Column text = new Column();
        text.setRecordStats(java.nio.ByteBuffer.wrap(PixelsProto.ColumnStatistic.newBuilder()
                .setStringStatistics(PixelsProto.StringStatistic.newBuilder().setSum(100)).build().toByteArray()));
        int position = text.getRecordStats().position();
        assertEquals(300, io.pixelsdb.pixels.planner.plan.physical.PartitionInputSizing.variableBytes(text));
        assertEquals(position, text.getRecordStats().position());
    }

    private static List<List<InputSplit>> plan(List<InputSplit> inputs, int memoryMB, int cpus, boolean streaming)
    {
        Column column = new Column(); column.setName("k"); column.setType("bigint");
        column.setChunkSize(MiB); column.setSize(10000D * MiB);
        return PartitionTaskPlanner.plan(inputs, Collections.singletonList(column), 10000L * 131072,
                new String[]{"k"}, new boolean[]{true}, 1024,
                PartitionMemoryPolicy.forResources(memoryMB * MiB, cpus), streaming, 2, MiB);
    }

    private static List<InputSplit> ranges(int groups, int size)
    {
        List<InputSplit> result = new ArrayList<>();
        for (int start = 0; start < groups; start += size)
        {
            result.add(new InputSplit(Collections.singletonList(new InputInfo("f", start, Math.min(size, groups - start)))));
        }
        return result;
    }

    private static void verifyRanges(List<List<InputSplit>> tasks, int groups)
    {
        int next = 0;
        for (List<InputSplit> task : tasks)
        {
            for (InputSplit split : task)
            {
                for (InputInfo input : split.getInputInfos())
                {
                    assertEquals(next, input.getRgStart()); next += input.getRgLength();
                }
            }
        }
        assertEquals(groups, next);
    }
}
