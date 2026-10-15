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
package io.pixelsdb.pixels.worker.spike;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.SpikeWorkerRequest;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.coordinate.CoordinatedPlanExecution;
import io.pixelsdb.pixels.planner.coordinate.CoordinatorEndpoint;
import io.pixelsdb.pixels.planner.coordinate.PlanCoordinatorFactory;
import io.pixelsdb.pixels.planner.coordinate.S3QSShuffleResourceLifecycle;
import io.pixelsdb.pixels.planner.coordinate.StageWorkerLauncher;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateServer;
import io.pixelsdb.pixels.planner.plan.physical.PartitionedJoinS3QSOperator;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedJoinInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleQueueInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import io.pixelsdb.pixels.worker.common.WorkerCommon;
import io.pixelsdb.spike.handler.SpikeWorker;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Runs a complete coordinator-managed S3QS partitioned join against real AWS.
 *
 * Platform deployment and its network hop are intentionally replaced by an
 * in-process StageWorkerLauncher. It still calls Spike's production request
 * handler and therefore exercises the real worker wrappers, coordinator gRPC
 * task protocol, partition/join workers, S3 object I/O, and SQS messaging.
 */
public class TestS3QSSpikeEndToEnd
{
    @Test
    public void coordinatedSpikeWorkersShuffleAndJoinThroughAws() throws Exception
    {
        String bucket = System.getenv("PIXELS_S3QS_IT_BUCKET");
        String prefix = trimSlashes(System.getenv("PIXELS_S3QS_IT_PREFIX"));
        String queuePrefix = System.getenv("PIXELS_S3QS_IT_QUEUE_PREFIX");
        assumeTrue("set PIXELS_S3QS_IT_BUCKET to run the AWS S3QS end-to-end test",
                bucket != null && !bucket.trim().isEmpty());
        assumeTrue("set PIXELS_S3QS_IT_QUEUE_PREFIX to run the AWS S3QS end-to-end test",
                queuePrefix != null && !queuePrefix.trim().isEmpty());

        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        long transId = Math.abs(UUID.randomUUID().getMostSignificantBits());
        long timestamp = System.currentTimeMillis();
        int coordinatorPort = freePort();
        String testRoot = bucket + "/" + (prefix.isEmpty() ? "" : prefix + "/") +
                "query-e2e-" + suffix + "/";
        String smallInputPath = testRoot + "input/small.pxl";
        String largeInputPath = testRoot + "input/large.pxl";
        String resultFolder = testRoot + "result/";
        String resultPath = resultFolder + "join.pxl";
        ShuffleInfo smallShuffle = shuffleInfo("small-" + suffix, testRoot + "shuffle/small/",
                queuePrefix + "-small-" + suffix);
        ShuffleInfo largeShuffle = shuffleInfo("large-" + suffix, testRoot + "shuffle/large/",
                queuePrefix + "-large-" + suffix);

        Storage s3 = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
        writeInput(s3, smallInputPath, "small_value", new long[][] {{1, 10}, {2, 20}});
        writeInput(s3, largeInputPath, "large_value", new long[][] {{1, 100}, {3, 300}});

        WorkerCoordinateServer coordinateServer = new WorkerCoordinateServer(coordinatorPort);
        Thread serverThread = new Thread(coordinateServer, "issue1277-e2e-coordinate-server");
        serverThread.setDaemon(true);
        serverThread.start();
        Thread.sleep(500L);

        CoordinatedPlanExecution execution = null;
        try
        {
            PartitionInput smallProducer = producerInput(transId, timestamp, "small", smallInputPath,
                    "small_value", smallShuffle);
            PartitionInput largeProducer = producerInput(transId, timestamp, "large", largeInputPath,
                    "large_value", largeShuffle);
            PartitionedJoinInput consumer = joinInput(transId, timestamp, smallShuffle, largeShuffle, resultFolder);
            PartitionedJoinS3QSOperator operator = new PartitionedJoinS3QSOperator(
                    "issue1277-s3qs-e2e",
                    Collections.singletonList(smallProducer),
                    Collections.singletonList(largeProducer),
                    Collections.<JoinInput>singletonList(consumer),
                    JoinAlgorithm.PARTITIONED);

            execution = PlanCoordinatorFactory.Instance().createPlanExecution(
                    transId, operator, new CoordinatorEndpoint("localhost", coordinatorPort),
                    new S3QSShuffleResourceLifecycle(), new InProcessSpikeStageWorkerLauncher());
            execution.execute();
            execution.collectOutputs();

            assertEquals(Collections.singleton("1:10:100"), readJoinedRows(s3, resultPath));
            assertShuffleObjectsDeleted(s3, smallShuffle.getObjectPathPrefix());
            assertShuffleObjectsDeleted(s3, largeShuffle.getObjectPathPrefix());
            S3QS s3qs = (S3QS) StorageFactory.Instance().getStorage(Storage.Scheme.s3qs);
            assertQueueDeleted(s3qs, smallShuffle.getQueues().get(0).getQueueName());
            assertQueueDeleted(s3qs, largeShuffle.getQueues().get(0).getQueueName());
        }
        finally
        {
            if (execution != null)
            {
                execution.close();
            }
            coordinateServer.shutdown();
            s3.delete(testRoot, true);
        }
    }

    private static PartitionInput producerInput(long transId, long timestamp, String tableName,
                                                String inputPath, String valueColumn, ShuffleInfo shuffleInfo)
    {
        String[] columns = {"key", valueColumn};
        ScanTableInfo tableInfo = new ScanTableInfo(tableName, true, columns,
                new StorageInfo(Storage.Scheme.s3, null, null, null, null),
                Collections.singletonList(new InputSplit(
                        Collections.singletonList(new InputInfo(inputPath, 0, 1)))),
                JSON.toJSONString(TableScanFilter.empty("issue1277", tableName)));
        OutputInfo output = new OutputInfo(shuffleInfo.getObjectPathPrefix(),
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null), true);
        output.setShuffleInfo(shuffleInfo);
        PartitionInput input = new PartitionInput(transId, timestamp, tableInfo,
                new boolean[] {true, true}, output, new PartitionInfo(new int[] {0}, 1));
        input.setProducerTaskId(0);
        return input;
    }

    private static PartitionedJoinInput joinInput(long transId, long timestamp,
                                                  ShuffleInfo smallShuffle, ShuffleInfo largeShuffle,
                                                  String resultFolder)
    {
        PartitionedTableInfo small = partitionedTable("small", "small_value", smallShuffle);
        PartitionedTableInfo large = partitionedTable("large", "large_value", largeShuffle);
        PartitionedJoinInfo joinInfo = new PartitionedJoinInfo(
                JoinType.EQUI_INNER,
                new String[] {"key", "small_value"},
                new String[] {"large_value"},
                new boolean[] {true, true},
                new boolean[] {false, true},
                false, null, 1, Collections.singletonList(0));
        MultiOutputInfo output = new MultiOutputInfo(resultFolder,
                new StorageInfo(Storage.Scheme.s3, null, null, null, null),
                true, Collections.singletonList("join.pxl"));
        return new PartitionedJoinInput(transId, timestamp, small, large, joinInfo,
                false, null, output);
    }

    private static PartitionedTableInfo partitionedTable(String tableName, String valueColumn,
                                                         ShuffleInfo shuffleInfo)
    {
        PartitionedTableInfo tableInfo = new PartitionedTableInfo(
                tableName, false, new String[] {"key", valueColumn},
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                Collections.emptyList(), 1, new int[] {0});
        tableInfo.setShuffleInfo(shuffleInfo);
        return tableInfo;
    }

    private static ShuffleInfo shuffleInfo(String shuffleId, String objectPrefix, String queueName)
    {
        return new ShuffleInfo(shuffleId,
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                objectPrefix, 1, 1, 1, 1,
                Collections.singletonList(new ShuffleQueueInfo(0, queueName, null)));
    }

    private static void writeInput(Storage storage, String path, String valueColumn, long[][] rows)
            throws IOException
    {
        TypeDescription schema = TypeDescription.createStruct()
                .addField("key", TypeDescription.createLong())
                .addField(valueColumn, TypeDescription.createLong());
        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector key = (LongColumnVector) batch.cols[0];
        LongColumnVector value = (LongColumnVector) batch.cols[1];
        for (long[] row : rows)
        {
            int rowId = batch.size++;
            key.vector[rowId] = row[0];
            value.vector[rowId] = row[1];
        }
        PixelsWriter writer = WorkerCommon.getWriter(schema, storage, path, true, false, null);
        writer.addRowBatch(batch);
        writer.close();
    }

    private static Set<String> readJoinedRows(Storage storage, String path) throws IOException
    {
        PixelsReaderOption option = new PixelsReaderOption()
                .skipCorruptRecords(true)
                .tolerantSchemaEvolution(true)
                .includeCols(new String[] {"key", "small_value", "large_value"});
        Set<String> rows = new HashSet<>();
        try (PixelsReader reader = WorkerCommon.getReader(path, storage);
             PixelsRecordReader recordReader = reader.read(option))
        {
            VectorizedRowBatch batch;
            do
            {
                batch = recordReader.readBatch(32);
                LongColumnVector key = (LongColumnVector) batch.cols[0];
                LongColumnVector smallValue = (LongColumnVector) batch.cols[1];
                LongColumnVector largeValue = (LongColumnVector) batch.cols[2];
                for (int i = 0; i < batch.size; ++i)
                {
                    rows.add(key.vector[i] + ":" + smallValue.vector[i] + ":" + largeValue.vector[i]);
                }
            }
            while (!batch.endOfFile);
        }
        return rows;
    }

    private static void assertShuffleObjectsDeleted(Storage storage, String prefix) throws Exception
    {
        for (int i = 0; i < 10; ++i)
        {
            if (!storage.exists(prefix))
            {
                return;
            }
            Thread.sleep(200L);
        }
        assertFalse("shuffle object prefix still exists: " + prefix, storage.exists(prefix));
    }

    private static void assertQueueDeleted(S3QS s3qs, String queueName) throws Exception
    {
        for (int i = 0; i < 20; ++i)
        {
            try
            {
                s3qs.getQueueUrl(queueName);
                Thread.sleep(500L);
            }
            catch (IOException expected)
            {
                return;
            }
        }
        fail("SQS queue still exists: " + queueName);
    }

    private static int freePort() throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0))
        {
            return socket.getLocalPort();
        }
    }

    private static String trimSlashes(String value)
    {
        if (value == null)
        {
            return "";
        }
        String trimmed = value.trim();
        while (trimmed.startsWith("/"))
        {
            trimmed = trimmed.substring(1);
        }
        while (trimmed.endsWith("/"))
        {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    private static class InProcessSpikeStageWorkerLauncher implements StageWorkerLauncher
    {
        private final AtomicLong requestIds = new AtomicLong(12770000L);

        @Override
        public CompletableFuture<? extends Output> launch(WorkerType workerType, StageWorkerInput input)
        {
            return CompletableFuture.supplyAsync(() ->
            {
                try
                {
                    SpikeWorkerRequest workerRequest = new SpikeWorkerRequest(workerType,
                            JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect));
                    SpikeWorker.CallWorkerFunctionReq request = SpikeWorker.CallWorkerFunctionReq.newBuilder()
                            .setRequestId(requestIds.getAndIncrement())
                            .setPayload(JSON.toJSONString(
                                    workerRequest, SerializerFeature.DisableCircularReferenceDetect))
                            .build();
                    String payload = new RequestHandlerImpl().execute(request).getPayload();
                    if (workerType == WorkerType.PARTITION_S3QS)
                    {
                        return JSON.parseObject(payload, PartitionOutput.class);
                    }
                    if (workerType == WorkerType.PARTITIONED_JOIN_S3QS)
                    {
                        return JSON.parseObject(payload, JoinOutput.class);
                    }
                    throw new IllegalArgumentException("unsupported test worker type: " + workerType);
                }
                catch (Throwable e)
                {
                    throw new CompletionException(e);
                }
            });
        }
    }
}
