/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.planner.coordinate;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.task.Task;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.planner.plan.physical.Operator;
import io.pixelsdb.pixels.planner.plan.physical.OperatorExecutor.OutputCollection;
import io.pixelsdb.pixels.turbo.TurboProto;
import org.junit.Test;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.*;

public class TestWorkerCancellationProtocol
{
    private static final AtomicLong IDS = new AtomicLong(127715100L);

    @Test
    public void abortRejectsClaimsCompletionsAndHeartbeatsButAcceptsStopEvidence()
    {
        PlanCoordinator plan = plan();
        WorkerCoordinateServiceImpl service = new WorkerCoordinateServiceImpl();
        try
        {
            String invocation = plan.getQueryState().reserveInvocation(17);
            Observer<TurboProto.RegisterWorkerResponse> registered = new Observer<>();
            service.registerWorker(registration(plan, invocation, 1), registered);
            assertEquals(ErrorCode.SUCCESS, registered.value.getErrorCode());
            long worker = registered.value.getWorkerId();
            Observer<TurboProto.GetTasksToExecuteResponse> claim = new Observer<>();
            service.getTasksToExecute(TurboProto.GetTasksToExecuteRequest.newBuilder().setWorkerId(worker).build(), claim);
            assertEquals(1, claim.value.getTaskInputsCount());
            RuntimeException original = new RuntimeException("producer failed before END");
            plan.getQueryState().abort(original);

            Observer<TurboProto.GetTasksToExecuteResponse> lateClaim = new Observer<>();
            service.getTasksToExecute(TurboProto.GetTasksToExecuteRequest.newBuilder().setWorkerId(worker).build(), lateClaim);
            assertEquals(ErrorCode.WORKER_COORDINATE_QUERY_ABORTED, lateClaim.value.getErrorCode());
            Observer<TurboProto.CompleteTasksResponse> complete = new Observer<>();
            service.completeTasks(TurboProto.CompleteTasksRequest.newBuilder().setWorkerId(worker)
                    .addTaskResults(TurboProto.TaskResult.newBuilder().setTaskId(0).setSuccess(true)).build(), complete);
            assertEquals(ErrorCode.WORKER_COORDINATE_QUERY_ABORTED, complete.value.getErrorCode());
            assertEquals(0, plan.getStageCoordinator(17).getCompletedTaskCount());
            Observer<TurboProto.ExtendLeaseResponse> heartbeat = new Observer<>();
            service.extendLease(TurboProto.ExtendLeaseRequest.newBuilder().setWorkerId(worker).build(), heartbeat);
            assertEquals(TurboProto.QueryStatus.QUERY_ABORTED, heartbeat.value.getQueryStatus());
            assertFalse(plan.getQueryState().isQuiescent());
            service.terminateWorker(TurboProto.TerminateWorkerRequest.newBuilder().setWorkerId(worker)
                    .setFailureMessage("secondary failure").setQuiescent(false).build(), new Observer<>());
            assertFalse(plan.getQueryState().isQuiescent());
            assertSame(original, plan.getQueryState().getFirstFailure());
            service.terminateWorker(TurboProto.TerminateWorkerRequest.newBuilder().setWorkerId(worker)
                    .setQuiescent(true).build(), new Observer<>());
            assertTrue(plan.getQueryState().isQuiescent());
        }
        finally { remove(plan); }
    }

    @Test
    public void incompatibleUnknownAndLateWorkersAreRejectedBeforeRegistration()
    {
        PlanCoordinator plan = plan();
        WorkerCoordinateServiceImpl service = new WorkerCoordinateServiceImpl();
        try
        {
            String invocation = plan.getQueryState().reserveInvocation(17);
            Observer<TurboProto.RegisterWorkerResponse> old = new Observer<>();
            service.registerWorker(registration(plan, invocation, 0), old);
            assertEquals(ErrorCode.WORKER_COORDINATE_PROTOCOL_MISMATCH, old.value.getErrorCode());
            Observer<TurboProto.RegisterWorkerResponse> unknown = new Observer<>();
            service.registerWorker(registration(plan, "unknown", 1), unknown);
            assertEquals(ErrorCode.WORKER_COORDINATE_TASK_REJECTED, unknown.value.getErrorCode());
            plan.getQueryState().abort(new RuntimeException("abort"));
            Observer<TurboProto.RegisterWorkerResponse> late = new Observer<>();
            service.registerWorker(registration(plan, invocation, 1), late);
            assertEquals(ErrorCode.WORKER_COORDINATE_QUERY_ABORTED, late.value.getErrorCode());
            assertEquals(0, plan.getStageCoordinator(17).getWorkers().size());
            assertEquals(1, plan.getStageCoordinator(17).getPendingTaskCount());
        }
        finally { remove(plan); }
    }

    @Test
    public void completionFromAnotherWorkerCannotStealTask()
    {
        PlanCoordinator plan = plan();
        WorkerCoordinateServiceImpl service = new WorkerCoordinateServiceImpl();
        try
        {
            Observer<TurboProto.RegisterWorkerResponse> first = new Observer<>();
            Observer<TurboProto.RegisterWorkerResponse> second = new Observer<>();
            service.registerWorker(registration(plan, plan.getQueryState().reserveInvocation(17), 1), first);
            service.registerWorker(registration(plan, plan.getQueryState().reserveInvocation(17), 1), second);
            service.getTasksToExecute(TurboProto.GetTasksToExecuteRequest.newBuilder()
                    .setWorkerId(first.value.getWorkerId()).build(), new Observer<>());
            Observer<TurboProto.CompleteTasksResponse> complete = new Observer<>();
            service.completeTasks(TurboProto.CompleteTasksRequest.newBuilder().setWorkerId(second.value.getWorkerId())
                    .addTaskResults(TurboProto.TaskResult.newBuilder().setTaskId(0).setSuccess(true)).build(), complete);
            assertEquals(ErrorCode.WORKER_COORDINATE_TASK_REJECTED, complete.value.getErrorCode());
            assertEquals(0, plan.getStageCoordinator(17).getCompletedTaskCount());
        }
        finally { remove(plan); }
    }

    private static TurboProto.RegisterWorkerRequest registration(PlanCoordinator plan, String invocation, int version)
    {
        return TurboProto.RegisterWorkerRequest.newBuilder().setWorkerInfo(TurboProto.WorkerInfo.newBuilder()
                .setIp("localhost").setPort(-1).setTransId(plan.getTransId()).setStageId(17)
                .setOperatorName(Constants.PARTITION_OPERATOR_NAME).setInvocationId(invocation)
                .setCancellationProtocolVersion(version)).build();
    }

    private static PlanCoordinator plan()
    {
        long id = IDS.getAndIncrement();
        return PlanCoordinatorFactory.Instance().createPlanCoordinator(id, new Operator("protocol-test")
        {
            public void initPlanCoordinator(PlanCoordinator plan, int parent, boolean wide)
            {
                StageCoordinator stage = new StageCoordinator(17, Collections.singletonList(new Task(0, "payload")));
                plan.addStageCoordinator(stage, new StageDependency(17, -1, false));
                plan.addStageRuntimeController(new StageExecutionDescriptor(id, 1, 17, "protocol-test",
                        WorkerType.PARTITION_S3QS, plan.getCoordinatorEndpoint()));
                stage.setDesiredRuntimeWorkerCount(2);
            }
            public CompletableFuture<CompletableFuture<? extends Output>[]> execute() { throw new UnsupportedOperationException(); }
            public CompletableFuture<Void> executePrev() { throw new UnsupportedOperationException(); }
            public OutputCollection collectOutputs() { throw new UnsupportedOperationException(); }
        }, new CoordinatorEndpoint("localhost", 18894));
    }

    private static void remove(PlanCoordinator plan)
    {
        plan.close();
        PlanCoordinatorFactory.Instance().removePlanCoordinator(plan.getTransId(), plan);
    }

    private static class Observer<T> implements StreamObserver<T>
    {
        T value;
        public void onNext(T value) { this.value = value; }
        public void onError(Throwable error) { throw new AssertionError(error); }
        public void onCompleted() { assertNotNull(value); }
    }
}
