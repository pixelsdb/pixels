/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.planner.coordinate;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.lease.Lease;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.task.Task;
import io.pixelsdb.pixels.common.task.Worker;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.plan.physical.PartitionedJoinS3QSOperator;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import org.junit.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestPreparedPlanResources
{
    private static final AtomicLong IDS = new AtomicLong(1277242000000L);

    @Test(timeout = 5000)
    public void serializedProducerAndConsumerTasksContainPreparedUrlsAndManifestOwnership() throws Exception
    {
        long id = IDS.getAndIncrement();
        SqsClient sqs = mock(SqsClient.class);
        S3Client s3 = mock(S3Client.class);
        S3QS storage = storage(sqs, s3);
        S3QSShuffleResourceLifecycle lifecycle = new S3QSShuffleResourceLifecycle(storage);
        PartitionInput small = partition("small"), large = partition("large");
        PartitionedJoinInput join = new PartitionedJoinInput();
        PartitionedTableInfo smallTable = new PartitionedTableInfo(), largeTable = new PartitionedTableInfo();
        // Deliberately separate equivalent references: preparation must update both sides.
        smallTable.setShuffleInfo(info("small"));
        largeTable.setShuffleInfo(info("large"));
        join.setSmallTable(smallTable);
        join.setLargeTable(largeTable);
        PartitionedJoinS3QSOperator root = new PartitionedJoinS3QSOperator("test",
                Collections.singletonList(small), Collections.singletonList(large),
                Collections.singletonList(join), JoinAlgorithm.PARTITIONED)
        {
            @Override
            public CompletableFuture<CompletableFuture<? extends Output>[]> execute()
            {
                return CompletableFuture.completedFuture(new CompletableFuture[0]);
            }
        };
        CoordinatedPlanExecution execution = PlanCoordinatorFactory.Instance().createPlanExecution(id, root,
                new CoordinatorEndpoint("localhost", 18894), lifecycle, (type, input) -> new CompletableFuture<>(),
                new QueryExecutionConfig(3000, 100, 500, Files.createTempDirectory("prepared-plan-")));
        try
        {
            execution.execute().get(1, TimeUnit.SECONDS);
            for (int stageId = 0; stageId < 3; stageId++)
            {
                StageCoordinator stage = execution.getPlanCoordinator().getStageCoordinator(stageId);
                stage.setDesiredRuntimeWorkerCount(1);
                Worker<CFWorkerInfo> worker = new Worker<>(stageId + 1,
                        new Lease(System.currentTimeMillis(), 60000), 0,
                        new CFWorkerInfo("localhost", 1, id, stageId, "test", Collections.emptyList()));
                stage.addWorker(worker);
                List<Task> tasks = stage.getTasksToRun(worker.getWorkerId());
                assertEquals(1, tasks.size());
                String payload = tasks.get(0).getPayload();
                if (stageId == 0)
                {
                    PartitionedJoinInput task = JSON.parseObject(payload, PartitionedJoinInput.class);
                    assertEquals("url/small-p0", task.getSmallTable().getShuffleInfo().getQueues().get(0).getQueueUrl());
                    assertEquals("url/large-p0", task.getLargeTable().getShuffleInfo().getQueues().get(0).getQueueUrl());
                }
                else
                {
                    PartitionInput task = JSON.parseObject(payload, PartitionInput.class);
                    assertEquals("url/" + (stageId == 1 ? "small" : "large") + "-p0",
                            task.getOutput().getShuffleInfo().getQueues().get(0).getQueueUrl());
                }
            }
            execution.collectOutputs();
            String manifest = new String(Files.readAllBytes(execution.getManifestPath()), StandardCharsets.UTF_8);
            assertTrue(manifest.contains("url/small-p0"));
            assertTrue(manifest.contains("url/large-p0"));
            assertTrue(manifest.contains("CLEANED"));
            verify(sqs).deleteQueue(DeleteQueueRequest.builder().queueUrl("url/small-p0").build());
            verify(sqs).deleteQueue(DeleteQueueRequest.builder().queueUrl("url/large-p0").build());
            verify(s3).listObjectsV2(argThat((ListObjectsV2Request request) -> request.prefix().equals("small/")));
            verify(s3).listObjectsV2(argThat((ListObjectsV2Request request) -> request.prefix().equals("large/")));
        }
        finally { execution.close(); }
    }

    @Test
    public void lateProgressListenerReceivesCompleteOwnershipSnapshot() throws Exception
    {
        S3QS storage = storage(mock(SqsClient.class), mock(S3Client.class));
        S3QSShuffleResourceLifecycle lifecycle = new S3QSShuffleResourceLifecycle(storage);
        lifecycle.prepare(Collections.singletonList(info("small")));
        AtomicReference<Map<String, Object>> snapshot = new AtomicReference<>();
        lifecycle.setProgressListener(snapshot::set);
        assertTrue(JSON.toJSONString(snapshot.get()).contains("url/small-p0"));
        lifecycle.cleanup();
        assertEquals(true, snapshot.get().get("cleaned"));
    }

    private static S3QS storage(SqsClient sqs, S3Client s3)
    {
        when(sqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenAnswer(call -> GetQueueUrlResponse.builder()
                .queueUrl("url/" + ((GetQueueUrlRequest) call.getArgument(0)).queueName()).build());
        when(s3.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder().keyCount(0).build());
        return new S3QS(s3, sqs, 30);
    }

    private static ShuffleInfo info(String name)
    {
        return new ShuffleInfo(name, new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                "bucket/" + name + "/", 1, 1, 1, 1,
                Collections.singletonList(new ShuffleQueueInfo(0, name + "-p0", null)));
    }

    private static PartitionInput partition(String name)
    {
        PartitionInput input = new PartitionInput();
        input.setProducerTaskId(0);
        OutputInfo output = new OutputInfo("bucket/" + name + "/", new StorageInfo(Storage.Scheme.s3qs, null, null, null, null), true);
        output.setShuffleInfo(info(name));
        input.setOutput(output);
        ScanTableInfo table = new ScanTableInfo();
        table.setInputSplits(Collections.emptyList());
        input.setTableInfo(table);
        return input;
    }
}
