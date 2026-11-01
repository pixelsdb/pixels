/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.PixelsPlanner;
import io.pixelsdb.pixels.planner.plan.logical.BaseTable;
import io.pixelsdb.pixels.planner.plan.physical.PartitionTaskPlanner;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/** Planning IO contracts, including the real PixelsPlanner partition-input entry point. */
public class TestPartitionPlanningIO
{
    @Test(timeout = 30000)
    public void s3qsPlannerNeverTouchesStorageWithOrWithoutPolicy() throws Exception
    {
        // PixelsPlanner snapshots exchange properties in static fields. Isolate
        // configurations from other tests, rather than altering those fields.
        for (String mode : Arrays.asList("false", "true"))
        {
            Path log = Files.createTempFile("partition-planner-", ".log");
            try
            {
                Process child = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                        "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                        getClass().getName(), mode).redirectErrorStream(true).redirectOutput(log.toFile()).start();
                try
                {
                    assertTrue(child.waitFor(10, TimeUnit.SECONDS));
                    String output = new String(Files.readAllBytes(log), java.nio.charset.StandardCharsets.UTF_8);
                    assertEquals(output, 0, child.exitValue());
                    assertTrue(output, output.contains("PLANNING_RESULT "));
                }
                finally { child.destroyForcibly(); }
            }
            finally { Files.deleteIfExists(log); }
        }
    }

    /** Local timing evidence for partition-input construction, not full SQL planning. */
    public static void main(String[] args) throws Exception
    {
        ConfigFactory config = ConfigFactory.Instance();
        config.addProperty("executor.exchange.method", "s3qs");
        config.addProperty("executor.shuffle.storage.scheme", "s3qs");
        config.addProperty("executor.intra.worker.parallelism", "8");
        config.addProperty("executor.partition.memory.planner.enabled", args[0]);
        Storage storage = mock(Storage.class, invocation -> {
            throw new AssertionError("S3QS planner attempted object storage IO: " + invocation.getMethod());
        });
        MetadataService metadata = mock(MetadataService.class);
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < 32; i++)
        {
            Column column = new Column(); column.setName("c" + i); column.setType("bigint");
            column.setChunkSize(1024 * 1024); column.setSize(2871D * 1024 * 1024); columns.add(column);
        }
        when(metadata.getColumns("test", "input", true)).thenReturn(columns);
        io.pixelsdb.pixels.common.metadata.domain.Table metadataTable = new io.pixelsdb.pixels.common.metadata.domain.Table();
        metadataTable.setRowCount(2871L * 131072); when(metadata.getTable("test", "input")).thenReturn(metadataTable);
        // Use the backend's cached resource interface; no real cloud invoker is loaded.
        Invoker invoker = mock(Invoker.class); when(invoker.getMemoryMB()).thenReturn(10240); when(invoker.getCpuCount()).thenReturn(6);
        Field invokers = InvokerFactory.class.getDeclaredField("invokerMap"); invokers.setAccessible(true);
        @SuppressWarnings("unchecked") Map<WorkerType, Invoker> backend = (Map<WorkerType, Invoker>) invokers.get(InvokerFactory.Instance());
        backend.put(WorkerType.PARTITION_S3QS, invoker);
        PixelsPlanner planner = mock(PixelsPlanner.class, CALLS_REAL_METHODS);
        for (String name : new String[]{"config", "storage", "metadataService"})
        {
            Field field = PixelsPlanner.class.getDeclaredField(name); field.setAccessible(true);
            field.set(planner, name.equals("config") ? config : name.equals("storage") ? storage : metadata);
        }
        Method plan = PixelsPlanner.class.getDeclaredMethod("getPartitionInputs",
                io.pixelsdb.pixels.planner.plan.logical.Table.class, List.class, int[].class, boolean[].class, int.class, String.class);
        plan.setAccessible(true);
        String[] names = new String[32]; boolean[] projection = new boolean[32]; Arrays.fill(projection, true);
        for (int i = 0; i < names.length; i++) { names[i] = "c" + i; }
        BaseTable table = new BaseTable("test", "input", "input", names, TableScanFilter.empty("test", "input"));
        List<InputSplit> left = ranges(30), right = ranges(2871);
        List<Double> samples = new ArrayList<>(); double coldMs = 0; int observedTasks = 0;
        for (int iteration = 0; iteration < 31; iteration++)
        {
            long begin = System.nanoTime();
            @SuppressWarnings("unchecked") List<PartitionInput> small = (List<PartitionInput>) plan.invoke(planner, table, left,
                    new int[]{0}, projection, 182, "bucket/left/");
            @SuppressWarnings("unchecked") List<PartitionInput> large = (List<PartitionInput>) plan.invoke(planner, table, right,
                    new int[]{0}, projection, 182, "bucket/right/");
            double elapsed = (System.nanoTime() - begin) / 1e6;
            if (iteration == 0) { coldMs = elapsed; }
            if (iteration > 10) { samples.add(elapsed); }
            assertEquals(1, small.size());
            if (!Boolean.parseBoolean(args[0])) { assertEquals(12, large.size()); }
            else { assertTrue(large.size() > 1); }
            observedTasks = small.size() + large.size();
            long expectedGroup = 0;
            for (PartitionInput input : large)
            {
                for (InputSplit split : input.getTableInfo().getInputSplits())
                {
                    for (InputInfo info : split.getInputInfos())
                    {
                        assertEquals(expectedGroup, info.getRgStart()); expectedGroup += info.getRgLength();
                    }
                }
            }
            assertEquals(2880, expectedGroup);
            for (int task = 0; task < large.size(); task++)
            {
                PartitionInput input = large.get(task);
                assertEquals(task, input.getProducerTaskId());
                assertEquals(large.size(), input.getOutput().getShuffleInfo().getLogicalProducerTaskCount());
                assertEquals(182, input.getPartitionInfo().getNumPartition());
                assertEquals(Boolean.parseBoolean(args[0]), input.getMemoryPolicy() != null);
            }
        }
        verifyNoInteractions(storage);
        if (!Boolean.parseBoolean(args[0])) { verifyNoInteractions(metadata); }
        Collections.sort(samples);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("policy", Boolean.parseBoolean(args[0])); result.put("rowGroups", 2901);
        result.put("producerTasks", observedTasks); result.put("storageCalls", 0);
        result.put("coldPartitionInputsMs", coldMs); result.put("warmP50PartitionInputsMs", samples.get(10));
        result.put("warmP95PartitionInputsMs", samples.get(18));
        System.out.println("PLANNING_RESULT " + JSON.toJSONString(result));
    }

    private static List<InputSplit> ranges(int groups)
    {
        List<InputSplit> result = new ArrayList<>();
        for (int start = 0; start < groups; start += 32)
        {
            result.add(new InputSplit(Collections.singletonList(new InputInfo("s3://unreachable/input", start, 32))));
        }
        return result;
    }
}
