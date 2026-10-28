/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.*;
import io.pixelsdb.pixels.core.vector.*;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.physical.PartitionTaskPlanner;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.storage.localfs.LocalFS;
import io.pixelsdb.pixels.storage.s3qs.*;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

/** Real Pixels files / scanner / partitioner / encoder; only AWS SDK calls are substituted. */
public class TestPartitionMemoryIntegration
{
    static final long MiB = 1024L * 1024;

    @Test(timeout = 30000)
    public void realVariableWidthFileSurvivesWindowsAndPreservesEveryValue() throws Exception
    {
        Path dir = Files.createTempDirectory("partition-windows-");
        try
        {
            Path input = dir.resolve("input.pxl");
            generate(input, 20000, 4, true, 16 * 1024);
            Map<String, Object> result = run(dir, "s3qs-worker", 20000, 4, true, 4, 2,
                    new PartitionMemoryPolicy(128 * MiB, 32 * MiB, 32 * MiB, 2, 2));
            assertEquals(JSON.toJSONString(result), true, result.get("successful"));
            assertEquals(20000L, result.get("validatedRows"));
            @SuppressWarnings("unchecked") List<Map<String, Long>> metrics = (List<Map<String, Long>>) result.get("memory");
            assertTrue(metrics.get(0).get("readWindows") > 1);
            for (Map<String, Long> metric : metrics)
            {
                assertTrue(metric.get("inputPeakBytes") <= metric.get("inputLimitBytes"));
                assertTrue(metric.get("outputPeakBytes") <= metric.get("outputLimitBytes"));
                assertEquals(Long.valueOf(0), metric.get("inputRemainingBytes"));
                assertEquals(Long.valueOf(0), metric.get("outputRemainingBytes"));
                assertEquals(Long.valueOf(0), metric.get("exactSizingWindows"));
            }
        }
        finally { delete(dir); }
    }

    @Test(timeout = 30000)
    public void actualPlatformMemoryReplacesStalePlannedMemoryAndAutoWindowsStayCorrect() throws Exception
    {
        Path dir = Files.createTempDirectory("partition-runtime-");
        try
        {
            generate(dir.resolve("input.pxl"), 30000, 4, true, 16 * 1024);
            Map<String, Object> result = run(dir, "s3qs-runtime", 30000, 4, true, 4, 4,
                    PartitionMemoryPolicy.forResources(256 * MiB, 4));
            assertEquals(JSON.toJSONString(result), true, result.get("successful"));
            assertEquals(30000L, result.get("validatedRows"));
            @SuppressWarnings("unchecked") List<Map<String, Long>> metrics = (List<Map<String, Long>>) result.get("memory");
            for (Map<String, Long> metric : metrics)
            {
                assertEquals(Long.valueOf(128 * MiB), metric.get("backendMemoryBytes"));
                assertEquals(Long.valueOf(0), metric.get("inputRemainingBytes"));
                assertEquals(Long.valueOf(0), metric.get("outputRemainingBytes"));
                assertTrue(metric.get("maxWindowRowGroups") > 4);
            }
        }
        finally { delete(dir); }
    }

    @Test(timeout = 30000)
    public void plannerMakesBatchRunnableAndKeepsS3QSLogicalTasksLarger() throws Exception
    {
        Path dir = Files.createTempDirectory("partition-plan-");
        try
        {
            generate(dir.resolve("input.pxl"), 20000, 4, false, 64 * 1024);
            PartitionMemoryPolicy policy = new PartitionMemoryPolicy(16 * MiB, 4 * MiB, 4 * MiB, 1, 2);
            Map<String, Object> batch = run(dir, "batch-hybrid", 20000, 4, false, 2, 1, policy);
            assertEquals(JSON.toJSONString(batch), true, batch.get("successful"));
            assertEquals(20000L, batch.get("validatedRows"));
            int groups;
            try (PixelsReader reader = reader(dir.resolve("input.pxl"), new LocalFS())) { groups = reader.getRowGroupNum(); }
            List<InputSplit> original = Collections.singletonList(new InputSplit(Collections.singletonList(
                    new InputInfo(dir.resolve("input.pxl").toString(), 0, groups))));
            List<List<InputSplit>> streaming = PartitionTaskPlanner.original(original, 8);
            assertTrue(((Number) batch.get("logicalTasks")).intValue() > streaming.size());
            assertEquals(groups, original.get(0).getInputInfos().get(0).getRgLength());
        }
        finally { delete(dir); }
    }

    @Test(timeout = 10000)
    public void metadataRejectsOversizeRGAndClosedWindowsDoNotAccumulate() throws Exception
    {
        Path dir = Files.createTempDirectory("partition-oversize-");
        try
        {
            Path input = dir.resolve("input.pxl"); generate(input, 10000, 4, false, 256 * 1024);
            PartitionMemoryPolicy tiny = new PartitionMemoryPolicy(128 * 1024, 32 * 1024, 32 * 1024, 1, 1);
            try (PixelsReader file = reader(input, new LocalFS()))
            {
                assertThrows(IOException.class, () -> PartitionReadWindow.next(file,
                        new InputInfo(input.toString(), 0, -1), 0, file.getRowGroupNum(),
                        schema(4, false).getFieldNames().toArray(new String[0]), tiny, tiny.getInputBytes()));
                for (int group = 0; group < file.getRowGroupNum(); group++)
                {
                    PixelsRecordReader records = file.read(new PixelsReaderOption().includeCols(new String[]{"c0"}).rgRange(group, 1));
                    while (!records.readBatch(1024).endOfFile) {}
                    file.closeRecordReader(records);
                    Field field = PixelsReaderImpl.class.getDeclaredField("recordReaders"); field.setAccessible(true);
                    assertEquals(0, ((List<?>) field.get(file)).size());
                }
            }
        }
        finally { delete(dir); }
    }

    public static void main(String[] args) throws Exception
    {
        ConfigFactory.Instance().addProperty("row.batch.size", "1024");
        ConfigFactory.Instance().addProperty("s3qs.producer.io.threads", "1");
        Path dir = Paths.get(args[1]); Files.createDirectories(dir);
        int rows = Integer.parseInt(args[2]), columns = Integer.parseInt(args[3]);
        if (args[0].equals("generate")) { generate(dir.resolve("input.pxl"), rows, columns, false, 4 * (int) MiB); return; }
        Map<String, Object> result = run(dir, args[0], rows, columns, false, 16, 8,
                args[0].contains("auto") ? PartitionMemoryPolicy.forResources(256 * MiB, Runtime.getRuntime().availableProcessors()) :
                        new PartitionMemoryPolicy(256 * MiB, 64 * MiB, 64 * MiB, 2, 4));
        System.out.println("MEMORY_RESULT " + JSON.toJSONString(result));
        if (!Boolean.TRUE.equals(result.get("successful"))) { System.exit(2); }
    }

    static Map<String, Object> run(Path dir, String mode, int rows, int columns, boolean binary,
                                   int partitions, int splits, PartitionMemoryPolicy policy) throws Exception
    {
        Map<String, Object> result = new LinkedHashMap<>(); result.put("mode", mode);
        Path input = dir.resolve("input.pxl"); TypeDescription schema = schema(columns, binary);
        String[] names = schema.getFieldNames().toArray(new String[0]); boolean[] projection = new boolean[names.length];
        Arrays.fill(projection, true); LocalFS local = new LocalFS();
        int groups; try (PixelsReader reader = reader(input, local)) { groups = reader.getRowGroupNum(); }
        List<InputSplit> ranges = new ArrayList<>();
        for (int start = 0; start < groups;)
        {
            int size = Math.max(1, (groups + splits - 1) / splits);
            ranges.add(new InputSplit(Collections.singletonList(new InputInfo(input.toString(), start, Math.min(size, groups - start)))));
            start += size;
        }
        List<Column> statistics;
        try (PixelsReader file = reader(input, local)) { statistics = statistics(file); }
        long planStart = System.nanoTime();
        boolean streaming = mode.startsWith("s3qs"), bounded = !mode.endsWith("legacy");
        List<List<InputSplit>> tasks = mode.endsWith("hybrid") ? PartitionTaskPlanner.plan(ranges, statistics, rows, names,
                projection, WorkerCommon.rowBatchSize, policy, streaming, partitions, 4L << 20) : PartitionTaskPlanner.original(ranges, 8);
        result.put("planningMs", (System.nanoTime() - planStart) / 1e6);
        result.put("logicalTasks", tasks.size()); result.put("rowGroups", groups); result.put("inputFileBytes", Files.size(input));
        Path objects = dir.resolve(mode + "-objects"); delete(objects); Files.createDirectories(objects);
        List<Map<String, Long>> memory = new ArrayList<>();
        long gcStart = gcTime(); long begin = System.nanoTime();
        AtomicBoolean stop = new AtomicBoolean(); AtomicLong peak = new AtomicLong();
        Thread sample = new Thread(() ->
        {
            while (!stop.get())
            {
                peak.accumulateAndGet(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(), Math::max);
                try { Thread.sleep(50); } catch (InterruptedException ignored) { break; }
            }
        }); sample.setDaemon(true); sample.start();
        boolean successful = true; String error = null;
        try (Sink sink = new Sink(objects, partitions, tasks.size()))
        {
            for (int task = 0; task < tasks.size(); task++)
            {
                StorageInfo in = new StorageInfo(Storage.Scheme.s3, null, null, null, null);
                ScanTableInfo table = new ScanTableInfo("test", true, names, in, tasks.get(task),
                        JSON.toJSONString(TableScanFilter.empty("test", "test")));
                OutputInfo output = new OutputInfo(objects.resolve("batch-" + task + ".pxl").toString(), in, false);
                if (streaming)
                {
                    output = new OutputInfo("bucket/shuffle/", sink.shuffle.getStorageInfo(), false);
                    output.setShuffleInfo(sink.shuffle);
                }
                PartitionInput event = new PartitionInput(1277, Long.MAX_VALUE, table, projection, output, new PartitionInfo(new int[]{0}, partitions));
                event.setProducerTaskId(task); if (bounded) { event.setMemoryPolicy(policy); }
                WorkerContext context = new WorkerContext(mock(Logger.class, withSettings().stubOnly()), new WorkerMetrics(), "memory-" + task)
                        .withCancellation(new WorkerCancellation());
                if (mode.equals("s3qs-runtime")) { context = context.withMemoryMB(128); }
                PartitionOutput response = new BasePartitionWorker(context).process(event);
                if (response.getMemoryMetrics() != null) { memory.add(response.getMemoryMetrics()); }
                if (!response.isSuccessful()) { successful = false; error = response.getErrorMessage(); break; }
            }
            result.put("producerMs", (System.nanoTime() - begin) / 1e6);
            result.put("gcMs", gcTime() - gcStart); result.put("sampledHeapPeakBytes", peak.get());
            result.put("memory", memory); result.put("successful", successful); result.put("error", error);
            result.put("objects", sink.sent.stream().filter(S3QueueMessage::isData).count());
            result.put("endMessages", sink.sent.stream().filter(S3QueueMessage::isProducerEnd).count());
            if (successful)
            {
                long verify = System.nanoTime();
                BitSet seen = new BitSet(rows);
                if (streaming)
                {
                    sink.verifyEnds(partitions, tasks.size());
                    for (S3QueueMessage message : sink.sent)
                    {
                        if (message.isData()) { verify(reader(message.getObjectPath(), sink.storage), rows, columns, binary, seen); }
                    }
                }
                else
                {
                    for (int task = 0; task < tasks.size(); task++) { verify(reader(objects.resolve("batch-" + task + ".pxl"), local), rows, columns, binary, seen); }
                }
                assertEquals(rows, seen.cardinality()); result.put("validatedRows", (long) seen.cardinality());
                result.put("validationMs", (System.nanoTime() - verify) / 1e6);
            }
        }
        finally { stop.set(true); sample.interrupt(); sample.join(); }
        return result;
    }

    static List<Column> statistics(PixelsReader file) throws Exception
    {
        List<Column> result = new ArrayList<>();
        for (int col = 0; col < file.getFileSchema().getChildren().size(); col++)
        {
            Column column = new Column(); column.setName(file.getFileSchema().getFieldNames().get(col));
            column.setType(file.getFileSchema().getChildren().get(col).toString());
            long bytes = 0;
            for (int rg = 0; rg < file.getRowGroupNum(); rg++)
            {
                bytes += Integer.toUnsignedLong(file.getRowGroupFooter(rg).getRowGroupIndexEntry().getColumnChunkIndexEntries(col).getChunkLength());
            }
            column.setSize(bytes); column.setChunkSize(bytes / (double) file.getRowGroupNum()); result.add(column);
        }
        return result;
    }

    static TypeDescription schema(int columns, boolean binary)
    {
        TypeDescription schema = TypeDescription.createStruct();
        for (int column = 0; column < columns; column++) { schema.addField("c" + column, TypeDescription.createLong()); }
        if (binary) { schema.addField("s", TypeDescription.createString()); } return schema;
    }

    static long value(long row, int column)
    {
        if (column == 0) { return row; }
        long x = row + column * 0x9e3779b97f4a7c15L;
        x = (x ^ (x >>> 30)) * 0xbf58476d1ce4e5b9L;
        x = (x ^ (x >>> 27)) * 0x94d049bb133111ebL; return x ^ (x >>> 31);
    }

    static void generate(Path path, int rows, int columns, boolean binary, int rgBytes) throws Exception
    {
        TypeDescription schema = schema(columns, binary);
        try (PixelsWriter writer = PixelsWriterImpl.newBuilder().setStorage(new LocalFS()).setPath(path.toString())
                .setSchema(schema).setRowGroupSize(rgBytes).setPixelStride(1024).setOverwrite(true).build())
        {
            for (int row = 0; row < rows;)
            {
                VectorizedRowBatch batch = schema.createRowBatch(1024);
                batch.size = Math.min(1024, rows - row);
                for (int i = 0; i < batch.size; i++)
                {
                    for (int c = 0; c < columns; c++) { ((LongColumnVector) batch.cols[c]).vector[i] = value(row + i, c); }
                    if (binary) { ((BinaryColumnVector) batch.cols[columns]).setVal(i, ("row-" + (row + i)).getBytes(java.nio.charset.StandardCharsets.UTF_8)); }
                }
                writer.addRowBatch(batch); row += batch.size;
            }
        }
    }

    static PixelsReader reader(Path path, Storage storage) throws IOException { return reader(path.toString(), storage); }
    static PixelsReader reader(String path, Storage storage) throws IOException
    {
        return PixelsReaderImpl.newBuilder().setStorage(storage).setPath(path).setPixelsFooterCache(new PixelsFooterCache()).build();
    }

    static void verify(PixelsReader reader, int rows, int columns, boolean binary, BitSet seen) throws Exception
    {
        try (PixelsReader file = reader; PixelsRecordReader records = file.read(new PixelsReaderOption().includeCols(
                schema(columns, binary).getFieldNames().toArray(new String[0]))))
        {
            while (true)
            {
                VectorizedRowBatch batch = records.readBatch(1024);
                for (int i = 0; i < batch.size; i++)
                {
                    long row = ((LongColumnVector) batch.cols[0]).vector[i];
                    assertTrue(row >= 0 && row < rows); assertFalse("duplicate row " + row, seen.get((int) row)); seen.set((int) row);
                    for (int c = 1; c < columns; c++) { assertEquals(value(row, c), ((LongColumnVector) batch.cols[c]).vector[i]); }
                    if (binary)
                    {
                        BinaryColumnVector values = (BinaryColumnVector) batch.cols[columns];
                        assertEquals("row-" + row, new String(values.vector[i], values.start[i], values.lens[i], java.nio.charset.StandardCharsets.UTF_8));
                    }
                }
                if (batch.endOfFile) { break; }
            }
        }
    }

    static long gcTime() { return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(bean -> Math.max(0, bean.getCollectionTime())).sum(); }
    static void delete(Path path) throws IOException
    {
        if (!Files.exists(path)) { return; }
        try (java.util.stream.Stream<Path> files = Files.walk(path))
        {
            for (Path file : (Iterable<Path>) files.sorted(Comparator.reverseOrder())::iterator) { Files.delete(file); }
        }
    }

    static final class Sink implements AutoCloseable
    {
        final Queue<S3QueueMessage> sent = new ConcurrentLinkedQueue<>();
        final S3Client s3 = mock(S3Client.class, withSettings().stubOnly());
        final SqsClient sqs = mock(SqsClient.class, withSettings().stubOnly());
        final S3QS storage = new S3QS(s3, sqs, 30);
        final ShuffleInfo shuffle;
        final Field cache;
        final Object previous;
        final Field inputCache;
        final Object previousInput;
        Sink(Path objects, int partitions, int producers) throws Exception
        {
            List<ShuffleQueueInfo> queues = new ArrayList<>();
            for (int p = 0; p < partitions; p++) { queues.add(new ShuffleQueueInfo(p, "test", "queue" + p)); storage.registerQueue("memory", p, "test", "queue" + p); }
            shuffle = new ShuffleInfo("memory", new StorageInfo(Storage.Scheme.s3qs, null, null, null, null), "bucket/shuffle/", partitions, producers, 1, 1, queues);
            when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call ->
            {
                PutObjectRequest request = call.getArgument(0); RequestBody body = call.getArgument(1);
                Path target = objects.resolve(request.key()); Files.createDirectories(target.getParent());
                try (InputStream stream = body.contentStreamProvider().newStream()) { Files.copy(stream, target, StandardCopyOption.REPLACE_EXISTING); }
                return PutObjectResponse.builder().build();
            });
            when(s3.getObject(any(GetObjectRequest.class), org.mockito.ArgumentMatchers.<ResponseTransformer<GetObjectResponse, ResponseBytes<GetObjectResponse>>>any()))
                    .thenAnswer(call -> ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), Files.readAllBytes(objects.resolve(((GetObjectRequest) call.getArgument(0)).key()))));
            when(sqs.sendMessage(any(SendMessageRequest.class))).thenAnswer(call ->
            {
                sent.add(S3QueueMessage.fromMessageBody(((SendMessageRequest) call.getArgument(0)).messageBody()));
                return SendMessageResponse.builder().build();
            });
            cache = WorkerCommon.class.getDeclaredField("s3qs"); cache.setAccessible(true); previous = cache.get(null); cache.set(null, storage);
            inputCache = WorkerCommon.class.getDeclaredField("s3"); inputCache.setAccessible(true);
            previousInput = inputCache.get(null); inputCache.set(null, new LocalFS());
        }
        void verifyEnds(int partitions, int producers)
        {
            assertEquals((long) partitions * producers, sent.stream().filter(S3QueueMessage::isProducerEnd).count());
            for (int producer = 0; producer < producers; producer++)
            {
                for (int partition = 0; partition < partitions; partition++)
                {
                    Set<Long> sequence = new HashSet<>(); Long boundary = null;
                    for (S3QueueMessage message : sent)
                    {
                        if (message.getProducerId() != producer || message.getPartitionId() != partition) { continue; }
                        if (message.isData()) { assertNull(boundary); assertTrue(sequence.add(message.getSequenceId())); }
                        else { assertNull(boundary); boundary = message.getSequenceId(); }
                    }
                    assertNotNull(boundary); assertEquals(sequence.size(), boundary.longValue());
                    for (long i = 0; i < boundary; i++) { assertTrue(sequence.contains(i)); }
                }
            }
        }
        @Override public void close() throws Exception
        {
            cache.set(null, previous); inputCache.set(null, previousInput); storage.close();
        }
    }
}
