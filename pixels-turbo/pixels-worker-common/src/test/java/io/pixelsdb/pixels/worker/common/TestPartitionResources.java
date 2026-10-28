/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.turbo.Invoker;
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
