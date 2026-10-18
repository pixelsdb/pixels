/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.Operator;
import io.pixelsdb.pixels.planner.plan.physical.OperatorExecutor.OutputCollection;
import io.pixelsdb.pixels.planner.plan.physical.ScanOperator.ScanOutputCollection;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import org.junit.Test;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.*;

public class TestQueryFailureLifecycle
{
    private static final AtomicLong IDS = new AtomicLong(127715000L);

    @Test(timeout = 5000)
    public void manifestFailurePreventsPreparationAndLaunch() throws Exception
    {
        long id = IDS.getAndIncrement();
        PlanCoordinatorFactory factory = PlanCoordinatorFactory.Instance();
        StubOperator operator = new StubOperator();
        Lifecycle lifecycle = new Lifecycle();
        PlanCoordinator plan = factory.createPlanCoordinator(id, operator, new CoordinatorEndpoint("localhost", 18894));
        // A regular file cannot be used as the required manifest directory.
        CoordinatedPlanExecution execution = new CoordinatedPlanExecution(id, operator, plan, factory,
                lifecycle, new QueryExecutionConfig(3000, 100, 500, Files.createTempFile("issue1277-invalid-manifest-", ".json")));
        try
        {
            execution.execute();
            try { execution.collectOutputs(); fail("manifest failure must be visible"); }
            catch (ExecutionException failure) { assertTrue(failure.getCause() instanceof java.io.UncheckedIOException); }
            assertEquals(CoordinatedPlanExecution.CleanupStatus.CLEANUP_FAILED,
                    execution.getCleanupCompletion().get(1, TimeUnit.SECONDS));
            assertEquals(0, lifecycle.prepareCount);
            assertEquals(0, lifecycle.cleanupCount);
        }
        finally { execution.close(); factory.removePlanCoordinator(id, plan); }
    }

    @Test(timeout = 5000)
    public void blockedPreparationCannotDelayDeadlineOrTriggerPrematureCleanup() throws Exception
    {
        CountDownLatch preparing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        Lifecycle lifecycle = new Lifecycle()
        {
            @Override
            public void prepare(Collection<ShuffleInfo> infos)
            {
                preparing.countDown();
                boolean done = false;
                while (!done)
                {
                    try { release.await(); done = true; }
                    catch (InterruptedException ignored) { /* Remote operation ignores interruption. */ }
                }
                exited.countDown();
            }
        };
        CoordinatedPlanExecution execution = execution(new StubOperator(), lifecycle, 150);
        try
        {
            execution.execute();
            assertTrue(preparing.await(1, TimeUnit.SECONDS));
            try { execution.collectOutputs(); fail("preparation must have a deadline"); }
            catch (ExecutionException failure) { assertTrue(failure.getCause() instanceof TimeoutException); }
            assertEquals(CoordinatedPlanExecution.CleanupStatus.CLEANUP_DEFERRED,
                    execution.getCleanupCompletion().get(1, TimeUnit.SECONDS));
            assertEquals(0, lifecycle.cleanupCount);
        }
        finally
        {
            release.countDown();
            assertTrue(exited.await(1, TimeUnit.SECONDS));
            execution.close();
            PlanCoordinatorFactory.Instance().removePlanCoordinator(
                    execution.getPlanCoordinator().getTransId(), execution.getPlanCoordinator());
        }
    }

    @Test(timeout = 5000)
    public void unknownRemoteDefersCleanupAndPersistsOriginalCause() throws Exception
    {
        Lifecycle lifecycle = new Lifecycle();
        StubOperator operator = new StubOperator();
        CoordinatedPlanExecution execution = execution(operator, lifecycle, 3000);
        QueryExecutionState state = execution.getPlanCoordinator().getQueryState();
        String id = state.reserveInvocation(3);
        assertTrue(state.registerInvocation(id, 3, 17));
        execution.execute().get(1, TimeUnit.SECONDS);
        RuntimeException original = new RuntimeException("producer failed before END");
        execution.abort(original);
        expectCause(execution, original);
        assertEquals(CoordinatedPlanExecution.CleanupStatus.CLEANUP_DEFERRED,
                execution.getCleanupCompletion().get(1, TimeUnit.SECONDS));
        assertEquals(0, lifecycle.cleanupCount);
        String manifest = new String(Files.readAllBytes(execution.getManifestPath()), StandardCharsets.UTF_8);
        assertTrue(manifest.contains("producer failed before END"));
        assertTrue(manifest.contains("CLEANUP_DEFERRED"));
        assertTrue(manifest.contains(id));
        // Test-only supervisor proves the fake invocation ended before removing its tombstone.
        state.workerStopped(id, 17);
        PlanCoordinatorFactory.Instance().removePlanCoordinator(
                execution.getPlanCoordinator().getTransId(), execution.getPlanCoordinator());
    }

    @Test(timeout = 5000)
    public void acknowledgedWorkerCanCleanAfterCallerAlreadyGotFailure() throws Exception
    {
        Lifecycle lifecycle = new Lifecycle();
        CoordinatedPlanExecution execution = execution(new StubOperator(), lifecycle, 3000);
        QueryExecutionState state = execution.getPlanCoordinator().getQueryState();
        String id = state.reserveInvocation(3);
        assertTrue(state.registerInvocation(id, 3, 17));
        execution.execute().get(1, TimeUnit.SECONDS);
        RuntimeException original = new RuntimeException("consumer failed");
        execution.abort(original);
        expectCause(execution, original);
        state.workerStopped(id, 17);
        assertEquals(CoordinatedPlanExecution.CleanupStatus.CLEANED,
                execution.getCleanupCompletion().get(1, TimeUnit.SECONDS));
        assertEquals(1, lifecycle.cleanupCount);
    }

    @Test(timeout = 5000)
    public void deadlineReleasesCollectorWaitingForMissingEnd() throws Exception
    {
        StubOperator operator = new StubOperator();
        operator.blockCollection = true;
        CoordinatedPlanExecution execution = execution(operator, new Lifecycle(), 150);
        execution.execute();
        try { execution.collectOutputs(); fail("deadline must fail"); }
        catch (ExecutionException expected) { assertTrue(expected.getCause() instanceof TimeoutException); }
        assertEquals(QueryExecutionState.Status.ABORTED, execution.getPlanCoordinator().getQueryState().getStatus());
        assertEquals(CoordinatedPlanExecution.CleanupStatus.CLEANED,
                execution.getCleanupCompletion().get(1, TimeUnit.SECONDS));
    }

    @Test(timeout = 5000)
    public void cleanupFailureNeverReplacesOriginalQueryCause() throws Exception
    {
        Lifecycle lifecycle = new Lifecycle();
        lifecycle.failCleanup = true;
        CoordinatedPlanExecution execution = execution(new StubOperator(), lifecycle, 3000);
        execution.execute().get(1, TimeUnit.SECONDS);
        RuntimeException original = new RuntimeException("original failure");
        execution.abort(original);
        expectCause(execution, original);
        assertEquals(CoordinatedPlanExecution.CleanupStatus.CLEANUP_FAILED,
                execution.getCleanupCompletion().get(1, TimeUnit.SECONDS));
        assertNotNull(execution.getCleanupFailure());
        assertSame(original, execution.getPlanCoordinator().getQueryState().getFirstFailure());
        PlanCoordinatorFactory.Instance().removePlanCoordinator(
                execution.getPlanCoordinator().getTransId(), execution.getPlanCoordinator());
    }

    private static void expectCause(CoordinatedPlanExecution execution, Throwable cause) throws Exception
    {
        try { execution.collectOutputs(); fail("query must fail"); }
        catch (ExecutionException expected) { assertSame(cause, expected.getCause()); }
    }

    private static CoordinatedPlanExecution execution(StubOperator operator, Lifecycle lifecycle, long timeout) throws Exception
    {
        long id = IDS.getAndIncrement();
        PlanCoordinatorFactory factory = PlanCoordinatorFactory.Instance();
        PlanCoordinator plan = factory.createPlanCoordinator(id, operator, new CoordinatorEndpoint("localhost", 18894));
        return new CoordinatedPlanExecution(id, operator, plan, factory, lifecycle,
                new QueryExecutionConfig(timeout, 100, 500, Files.createTempDirectory("issue1277-manifest-test-")));
    }

    private static class Lifecycle implements ShuffleResourceLifecycle
    {
        volatile int prepareCount;
        volatile int cleanupCount;
        boolean failCleanup;
        public void prepare(Collection<ShuffleInfo> infos) { prepareCount++; }
        public void cleanup() throws IOException
        {
            cleanupCount++;
            if (failCleanup) { throw new IOException("delete denied"); }
        }
    }

    private static class StubOperator extends Operator
    {
        boolean blockCollection;
        StubOperator() { super("failure-test"); }
        public void initPlanCoordinator(PlanCoordinator plan, int parent, boolean wide) {}
        public CompletableFuture<CompletableFuture<? extends Output>[]> execute()
        { return CompletableFuture.completedFuture(new CompletableFuture[0]); }
        public CompletableFuture<Void> executePrev() { return CompletableFuture.completedFuture(null); }
        public OutputCollection collectOutputs() throws InterruptedException
        {
            if (blockCollection) { new CountDownLatch(1).await(); }
            return new ScanOutputCollection();
        }
    }
}
