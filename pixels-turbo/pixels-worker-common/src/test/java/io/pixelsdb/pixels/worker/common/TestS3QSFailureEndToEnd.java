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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.task.Task;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.planner.coordinate.*;
import io.pixelsdb.pixels.planner.plan.physical.Operator;
import io.pixelsdb.pixels.planner.plan.physical.OperatorExecutor.OutputCollection;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import org.apache.logging.log4j.LogManager;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

/** Real runner + TCP coordinator + consumer poll loop; only the AWS transport is simulated. */
public class TestS3QSFailureEndToEnd
{
    @Test(timeout = 5000)
    public void unresponsiveCoordinatorRpcHasItsOwnDeadline() throws Exception
    {
        Server server = ServerBuilder.forPort(0).addService(new WorkerCoordinateServiceImpl()
        {
            @Override
            public void extendLease(io.pixelsdb.pixels.turbo.TurboProto.ExtendLeaseRequest request,
                    io.grpc.stub.StreamObserver<io.pixelsdb.pixels.turbo.TurboProto.ExtendLeaseResponse> response)
            {
                // Simulate a reachable server that never answers this RPC.
            }
        }).build().start();
        WorkerCoordinateService client = new WorkerCoordinateService("127.0.0.1", server.getPort(), 100);
        try
        {
            long start = System.nanoTime();
            try { client.extendLease(1); fail("RPC must time out"); }
            catch (io.grpc.StatusRuntimeException failure)
            {
                assertEquals(io.grpc.Status.Code.DEADLINE_EXCEEDED, failure.getStatus().getCode());
            }
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 2000);
        }
        finally
        {
            client.shutdown();
            server.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20000)
    public void producerFailureCancelsPollingConsumerViaDefaultLeaseHeartbeat() throws Exception
    {
        Server server = ServerBuilder.forPort(0).addService(new WorkerCoordinateServiceImpl()).build().start();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        SqsClient sqs = mock(SqsClient.class);
        S3QS storage = new S3QS(mock(S3Client.class), sqs, 30);
        storage.registerQueue("heartbeat-abort", 0, "ignored", "queue-url");
        Field storageField = WorkerCommon.class.getDeclaredField("s3qs");
        storageField.setAccessible(true);
        Object previousStorage = storageField.get(null);
        storageField.set(null, storage);
        CountDownLatch consumerPolling = new CountDownLatch(1);
        AtomicBoolean consumerExited = new AtomicBoolean();
        AtomicInteger cleanupCount = new AtomicInteger();
        List<CompletableFuture<? extends Output>> invocations = Collections.synchronizedList(new ArrayList<>());
        when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(call ->
        {
            consumerPolling.countDown();
            Thread.sleep(20);
            return ReceiveMessageResponse.builder().build();
        });
        ShuffleInfo shuffle = new ShuffleInfo("heartbeat-abort",
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                "bucket/heartbeat-abort/", 1, 1, 20, 1, Collections.emptyList());
        CoordinatorEndpoint endpoint = new CoordinatorEndpoint("127.0.0.1", server.getPort());
        long transId = System.nanoTime();
        StageWorkerLauncher launcher = (type, input) ->
        {
            CompletableFuture<Output> invocation = CompletableFuture.supplyAsync(() ->
            {
                WorkerContext context = new WorkerContext(LogManager.getLogger(TestS3QSFailureEndToEnd.class),
                        new WorkerMetrics(), "failure-" + input.getInvocationId());
                S3QSStageWorkerRunner runner = new S3QSStageWorkerRunner(context);
                boolean producer = type == WorkerType.PARTITION_S3QS;
                return runner.run(input, producer ? Constants.PARTITION_OPERATOR_NAME : Constants.PARTITION_JOIN_OPERATOR_NAME,
                        type, String.class, (payload, taskContext) ->
                        {
                            if (producer)
                            {
                                try { assertTrue(consumerPolling.await(2, TimeUnit.SECONDS)); }
                                catch (InterruptedException error) { throw new WorkerException("interrupted producer", error); }
                                throw new WorkerException("producer-before-END-original");
                            }
                            try
                            {
                                BasePartitionedJoinWorker.drainS3QSPartition(shuffle, 0, new HashMap<>(),
                                        message -> {}, taskContext.getCancellation());
                                throw new AssertionError("consumer must not succeed without END");
                            }
                            catch (Exception error) { throw new WorkerException("consumer cancelled", error); }
                            finally { consumerExited.set(true); }
                        }, new PartitionOutput());
            }, workers);
            invocations.add(invocation);
            return invocation;
        };
        Operator operator = new Operator("heartbeat-abort-plan")
        {
            private PlanCoordinator coordinator;
            public void initPlanCoordinator(PlanCoordinator plan, int parent, boolean wide)
            {
                coordinator = plan;
                for (int stage = 0; stage < 2; stage++)
                {
                    plan.addStageCoordinator(new StageCoordinator(stage,
                            Collections.singletonList(new Task(0, "\"payload\""))), new StageDependency(stage, -1, false));
                    plan.addStageRuntimeController(new StageExecutionDescriptor(transId, 1, stage, "heartbeat-stage",
                            stage == 0 ? WorkerType.PARTITIONED_JOIN_S3QS : WorkerType.PARTITION_S3QS, endpoint));
                }
            }
            public CompletableFuture<CompletableFuture<? extends Output>[]> execute()
            {
                coordinator.activateStage(0);
                coordinator.activateStage(1);
                return CompletableFuture.completedFuture(new CompletableFuture[0]);
            }
            public CompletableFuture<Void> executePrev() { return CompletableFuture.completedFuture(null); }
            public OutputCollection collectOutputs() throws InterruptedException
            {
                new CountDownLatch(1).await();
                throw new AssertionError("collector must be interrupted");
            }
        };
        ShuffleResourceLifecycle lifecycle = new ShuffleResourceLifecycle()
        {
            public void prepare(Collection<ShuffleInfo> infos) {}
            public void cleanup()
            {
                assertTrue("cleanup requires actual consumer exit", consumerExited.get());
                cleanupCount.incrementAndGet();
            }
        };
        CoordinatedPlanExecution execution = PlanCoordinatorFactory.Instance().createPlanExecution(transId,
                operator, endpoint, lifecycle, launcher,
                new QueryExecutionConfig(10000, 7000, 1000, Files.createTempDirectory("issue1277-heartbeat-it-")));
        try
        {
            execution.execute();
            try { execution.collectOutputs(); fail("producer failure must return"); }
            catch (ExecutionException failure)
            {
                assertTrue(failure.getCause().toString(), failure.getCause().toString().contains("producer-before-END-original"));
            }
            assertEquals(CoordinatedPlanExecution.CleanupStatus.CLEANED,
                    execution.getCleanupCompletion().get(9, TimeUnit.SECONDS));
            assertTrue(consumerExited.get());
            assertEquals(1, cleanupCount.get());
            assertEquals(0, execution.getPlanCoordinator().getStageCoordinator(0).getCompletedTaskCount());
            assertTrue(execution.getPlanCoordinator().getQueryState().isQuiescent());
            for (CompletableFuture<? extends Output> invocation : invocations)
            {
                try { invocation.get(1, TimeUnit.SECONDS); fail("both workers must fail"); }
                catch (ExecutionException expected) { /* Actual handler exit, not future cancellation. */ }
            }
        }
        finally
        {
            execution.close();
            workers.shutdownNow();
            workers.awaitTermination(2, TimeUnit.SECONDS);
            storageField.set(null, previousStorage);
            storage.close();
            server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
    }
}
