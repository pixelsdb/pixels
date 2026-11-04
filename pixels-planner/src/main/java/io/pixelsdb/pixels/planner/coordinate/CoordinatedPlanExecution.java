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
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.cleanup.CleanupResource;
import io.pixelsdb.pixels.common.turbo.cleanup.TemporaryResourceManager;
import io.pixelsdb.pixels.planner.plan.physical.Operator;
import io.pixelsdb.pixels.planner.plan.physical.OperatorExecutor.OutputCollection;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/** Query result deadlines and remote quiescence are independent lifecycles. */
public class CoordinatedPlanExecution implements AutoCloseable
{
    public enum CleanupStatus { NOT_STARTED, WAITING_FOR_QUIESCENCE, CLEANING, RETRY_WAIT, NEEDS_ATTENTION, CLEANED,
        CLEANUP_DEFERRED, CLEANUP_FAILED }

    private final long transId;
    private volatile Operator rootOperator;
    private volatile PlanCoordinator planCoordinator;
    private final PlanCoordinatorFactory coordinatorFactory;
    private final ShuffleResourceLifecycle shuffleResourceLifecycle;
    private final QueryExecutionConfig config;
    private volatile Runnable initializeStages;
    private final QueryCleanupManifest manifest;
    private final ExecutorService executor;
    private final ScheduledExecutorService timer;
    private final CompletableFuture<CompletableFuture<? extends Output>[]> executionFuture = new CompletableFuture<>();
    private final CompletableFuture<OutputCollection> collected = new CompletableFuture<>();
    private final CompletableFuture<CleanupStatus> cleanupCompletion = new CompletableFuture<>();
    private final CompletableFuture<Void> computeCompletion = new CompletableFuture<>();
    private final List<String> resultScopes = new ArrayList<>();
    private volatile TemporaryResourceManager resourceManager;
    private volatile String shuffleScope;
    private final AtomicBoolean collecting = new AtomicBoolean();
    private final AtomicBoolean cleanupStarted = new AtomicBoolean();
    private volatile CleanupStatus cleanupStatus = CleanupStatus.NOT_STARTED;
    private volatile Map<String, Object> resources = Collections.emptyMap();
    private volatile Throwable cleanupFailure;
    private boolean executed;
    private volatile boolean preparationFinished = true;
    private volatile boolean preparationStarted;
    private volatile boolean resourcesPrepared;
    private volatile boolean closed;
    private volatile boolean cleanupRequested;
    private volatile Future<?> startupTask;
    private volatile Future<?> collectionTask;
    private boolean detached;

    CoordinatedPlanExecution(long transId, Operator rootOperator, PlanCoordinator planCoordinator,
                             PlanCoordinatorFactory coordinatorFactory, ShuffleResourceLifecycle lifecycle)
    {
        this(transId, rootOperator, planCoordinator, coordinatorFactory, lifecycle, QueryExecutionConfig.fromConfig());
    }

    CoordinatedPlanExecution(long transId, Operator rootOperator, PlanCoordinator planCoordinator,
                             PlanCoordinatorFactory coordinatorFactory, ShuffleResourceLifecycle lifecycle,
                             QueryExecutionConfig config)
    {
        this(transId, rootOperator, planCoordinator, coordinatorFactory, lifecycle, config, () -> {});
    }

    CoordinatedPlanExecution(long transId, Operator rootOperator, PlanCoordinator planCoordinator,
                             PlanCoordinatorFactory coordinatorFactory, ShuffleResourceLifecycle lifecycle,
                             QueryExecutionConfig config, Runnable initializeStages)
    {
        this(transId, rootOperator, planCoordinator, coordinatorFactory, lifecycle, config, initializeStages, null);
    }

    CoordinatedPlanExecution(long transId, Operator rootOperator, PlanCoordinator planCoordinator,
                             PlanCoordinatorFactory coordinatorFactory, ShuffleResourceLifecycle lifecycle,
                             QueryExecutionConfig config, Runnable initializeStages, TemporaryResourceManager resourceManager)
    {
        this.resourceManager = resourceManager;
        this.initializeStages = requireNonNull(initializeStages, "initializeStages is null");
        this.transId = transId;
        this.rootOperator = requireNonNull(rootOperator, "rootOperator is null");
        this.planCoordinator = requireNonNull(planCoordinator, "planCoordinator is null");
        this.coordinatorFactory = requireNonNull(coordinatorFactory, "coordinatorFactory is null");
        this.shuffleResourceLifecycle = requireNonNull(lifecycle, "lifecycle is null");
        this.config = requireNonNull(config, "config is null");
        manifest = new QueryCleanupManifest(config.manifestDirectory, transId);
        ThreadFactory threads = task ->
        {
            Thread thread = new Thread(task, "s3qs-query-" + transId);
            thread.setDaemon(true);
            return thread;
        };
        executor = Executors.newCachedThreadPool(threads);
        timer = Executors.newSingleThreadScheduledExecutor(threads);
        executionFuture.whenComplete((value, failure) ->
        {
            if (executionFuture.isCancelled()) { abort(new CancellationException("execution future cancelled")); }
        });
        planCoordinator.getQueryState().setInvocationRecorder(this::persistManifest);
        lifecycle.setProgressListener(snapshot ->
        {
            resources = snapshot;
            persistManifest();
        });
        planCoordinator.getQueryState().getQuiescenceFuture().thenRun(this::scheduleCleanup);
        planCoordinator.getQueryState().getFailureFuture().whenComplete((ignored, error) ->
        {
            Throwable cause = this.planCoordinator.getQueryState().getFirstFailure();
            executionFuture.completeExceptionally(cause);
            collected.completeExceptionally(cause);
            Future<?> startup = startupTask;
            // A queued task must run its finally block. Cancelling it before entry would
            // leave preparationFinished false forever and strand late stop acknowledgements.
            if (startup != null && preparationStarted) { startup.cancel(true); }
            Future<?> collector = collectionTask;
            if (collector != null) { collector.cancel(true); }
            executor.execute(this::closeAfterFailure);
        });
    }

    public CompletableFuture<CompletableFuture<? extends Output>[]> execute()
    {
        synchronized (this)
        {
            checkState(!closed, "plan execution is closed");
            checkState(!executed, "plan execution has already started");
            executed = true;
            preparationFinished = false;
        }
        try
        {
            timer.schedule(() -> abort(new TimeoutException("S3QS query deadline exceeded after " +
                    config.timeoutMs + "ms")), config.timeoutMs, TimeUnit.MILLISECONDS);
            startupTask = executor.submit(this::prepareAndExecute);
            if (preparationStarted && planCoordinator.getQueryState().getStatus() == QueryExecutionState.Status.ABORTED)
            {
                startupTask.cancel(true);
            }
        }
        catch (RejectedExecutionException failure)
        {
            // close() can win between admission and dispatch. No preparation
            // task was accepted in that case, so no remote work can start here.
            preparationFinished = true;
            abort(failure);
        }
        return executionFuture;
    }

    private void prepareAndExecute()
    {
        preparationStarted = true;
        try
        {
            persistManifest();
            planCoordinator.getQueryState().requireRunning();
            List<CleanupResource> intent = shuffleResourceLifecycle.describeResources(planCoordinator.getShuffleInfos());
            if (!intent.isEmpty())
            {
                shuffleScope = resources().register("query-" + transId + "/shuffle", intent);
                persistManifest();
            }
            planCoordinator.getQueryState().requireRunning();
            resourcesPrepared = true;
            shuffleResourceLifecycle.prepare(planCoordinator.getShuffleInfos());
            planCoordinator.getQueryState().requireRunning();
            initializeStages.run();
            planCoordinator.getQueryState().requireRunning();
            CompletableFuture<CompletableFuture<? extends Output>[]> started = rootOperator.execute();
            preparationFinished = true;
            started.whenComplete((outputs, failure) ->
            {
                if (failure != null) { abort(failure); }
                else { executionFuture.complete(outputs); }
            });
        }
        catch (Throwable failure) { abort(failure); }
        finally
        {
            preparationFinished = true;
            if (closed) { detachPlan(); scheduleCleanup(); }
        }
    }

    public OutputCollection collectOutputs() throws ExecutionException, InterruptedException
    {
        synchronized (this) { checkState(executed, "plan execution has not started"); }
        if (collecting.compareAndSet(false, true))
        {
            executionFuture.whenComplete((ignored, failure) ->
            {
                if (failure == null)
                {
                    collectionTask = executor.submit(() ->
                    {
                        try
                        {
                            planCoordinator.getQueryState().requireRunning();
                            OutputCollection result = rootOperator.collectOutputs();
                            if (planCoordinator.getQueryState().succeed()) { collected.complete(result); }
                        }
                        catch (Throwable error) { abort(error); }
                    });
                    if (planCoordinator.getQueryState().getStatus() == QueryExecutionState.Status.ABORTED)
                    {
                        collectionTask.cancel(true);
                    }
                }
            });
        }
        try
        {
            OutputCollection result = collected.get();
            close();
            return result;
        }
        catch (InterruptedException failure)
        {
            abort(failure);
            Thread.currentThread().interrupt();
            throw failure;
        }
    }

    public boolean abort(Throwable failure) { return planCoordinator.getQueryState().abort(failure); }
    public PlanCoordinator getPlanCoordinator() { return planCoordinator; }
    public CleanupStatus getCleanupStatus()
    {
        if (shuffleScope == null) { return cleanupStatus; }
        try
        {
            String status = resources().snapshot(shuffleScope).getString("status");
            switch (TemporaryResourceManager.Status.valueOf(status))
            {
                case CLEANED: return CleanupStatus.CLEANED;
                case RETRY_WAIT: return CleanupStatus.RETRY_WAIT;
                case NEEDS_ATTENTION: return CleanupStatus.NEEDS_ATTENTION;
                case CLEANING: return CleanupStatus.CLEANING;
                default: return cleanupStatus;
            }
        }
        catch (IOException failure) { throw new UncheckedIOException(failure); }
    }
    public CompletableFuture<CleanupStatus> getCleanupCompletion() { return cleanupCompletion; }
    /** All owned work has stopped; a platform invocation/transport epilogue may still be returning. */
    public CompletableFuture<Void> getComputeCompletion() { return computeCompletion; }
    public String getCleanupScope() { return shuffleScope; }
    public boolean hasResultResources() { return !resultScopes.isEmpty(); }
    public Path getManifestPath() { return manifest.getPath(); }
    public Throwable getCleanupFailure() { return cleanupFailure; }

    private TemporaryResourceManager resources()
    {
        if (resourceManager == null) { resourceManager = TemporaryResourceManager.Instance(); }
        return resourceManager;
    }

    /** Register final temporary output BEFORE execution. Persistent table outputs must never be passed here. */
    public synchronized String registerResultResources(Collection<CleanupResource> intent) throws IOException
    {
        checkState(!executed && !closed, "result resources must be registered before execution");
        String scope = resources().register("query-" + transId + "/result", intent);
        resultScopes.add(scope);
        return scope;
    }

    /** Transaction/reader teardown signals release; it does not run physical deletion. */
    public void releaseResultResources() throws IOException
    {
        for (String scope : resultScopes) { resources().release(scope); }
        if (!resultScopes.isEmpty()) { resources().start(); }
    }

    @Override
    public void close()
    {
        synchronized (this)
        {
            if (closed) { return; }
            closed = true;
        }
        if (planCoordinator.getQueryState().getStatus() == QueryExecutionState.Status.RUNNING)
        {
            abort(new CancellationException("query execution closed"));
        }
        planCoordinator.close();
        timer.shutdownNow();
        cleanupStatus = CleanupStatus.WAITING_FOR_QUIESCENCE;
        try
        {
            persistManifest();
            if (shuffleScope != null) { resources().release(shuffleScope); resources().start(); }
        }
        catch (IOException failure) { throw new UncheckedIOException("failed to hand off cleanup", failure); }
        finally { cleanupRequested = true; detachPlan(); scheduleCleanup(); }
    }

    private synchronized void detachPlan()
    {
        if (detached || !closed || !preparationFinished) { return; }
        detached = true;
        PlanCoordinator previous = planCoordinator;
        planCoordinator = previous.terminalTombstone();
        coordinatorFactory.retainTerminalCoordinator(transId, previous);
        rootOperator = null;
        initializeStages = null;
    }

    private void scheduleCleanup()
    {
        if (!cleanupRequested || !preparationFinished || !planCoordinator.getQueryState().isQuiescent()) { return; }
        if (!cleanupStarted.compareAndSet(false, true)) { return; }
        executor.execute(() ->
        {
            try { finishCleanup(); }
            catch (Throwable failure) { recordCleanupFailure(failure); }
        });
    }

    private void finishCleanup() throws IOException
    {
        // This is positive stop evidence; no timeout or cancelled SDK future can reach this point.
        try { shuffleResourceLifecycle.closeLocalResources(); }
        catch (IOException failure) { cleanupFailure = failure; } // Local handle failure cannot revoke stop evidence.
        coordinatorFactory.removePlanCoordinator(transId, planCoordinator);
        computeCompletion.complete(null);
        for (String scope : resultScopes) { resources().confirmStoppedAsync(scope); }
        if (shuffleScope != null)
        {
            // Preparation may have published its scope after close() first ran.
            resources().releaseAsync(shuffleScope);
            resources().confirmStoppedAsync(shuffleScope);
            CompletableFuture<CleanupStatus> completion = cleanupCompletion;
            // Keep only a small future in the shared manager, not this query's entire operator DAG.
            resources().completion(shuffleScope).thenRun(() -> completion.complete(CleanupStatus.CLEANED));
            cleanupStatus = CleanupStatus.CLEANING;
            resources().start();
        }
        else
        {
            // Compatibility for lifecycles without external resources (including embedded test operators).
            if (resourcesPrepared) { shuffleResourceLifecycle.cleanup(); }
            cleanupStatus = CleanupStatus.CLEANED;
        }
        persistManifest();
        if (shuffleScope == null) { cleanupCompletion.complete(CleanupStatus.CLEANED); }
        executor.shutdown();
    }

    private void closeAfterFailure()
    {
        try { close(); }
        catch (Throwable failure) { recordCleanupFailure(failure); }
    }

    private void recordCleanupFailure(Throwable failure)
    {
        cleanupFailure = failure;
        CleanupStatus failureStatus = shuffleScope == null ? CleanupStatus.CLEANUP_FAILED : CleanupStatus.RETRY_WAIT;
        cleanupStatus = failureStatus;
        try { persistManifest(); }
        catch (RuntimeException manifestFailure) { failure.addSuppressed(manifestFailure); }
        coordinatorFactory.retainTerminalCoordinator(transId, planCoordinator);
        if (failureStatus == CleanupStatus.CLEANUP_FAILED) { cleanupCompletion.complete(failureStatus); }
        executor.shutdown();
    }

    private void persistManifest()
    {
        synchronized (manifest)
        {
            Map<String, Object> snapshot = new LinkedHashMap<>();
            snapshot.put("transId", transId);
            snapshot.put("queryStatus", planCoordinator.getQueryState().getStatus().name());
            Throwable cause = planCoordinator.getQueryState().getFirstFailure();
            snapshot.put("firstFailure", cause == null ? null : cause.toString());
            snapshot.put("cleanupStatus", cleanupStatus.name());
            snapshot.put("cleanupFailure", cleanupFailure == null ? null : cleanupFailure.toString());
            snapshot.put("invocations", planCoordinator.getQueryState().snapshotInvocations());
            snapshot.put("resources", resources);
            snapshot.put("cleanupScope", shuffleScope);
            snapshot.put("cleanupJournal", resourceManager == null ? null : resourceManager.getJournalLocation());
            snapshot.put("resultScopes", resultScopes);
            snapshot.put("cancellationProtocolVersion", 1);
            snapshot.put("artifactId", System.getProperty("pixels.artifact.id", "local-build-unspecified"));
            snapshot.put("queryTimeoutMs", config.timeoutMs);
            snapshot.put("abortGraceMs", config.abortGraceMs);
            snapshot.put("cleanupTimeoutMs", config.cleanupTimeoutMs);
            snapshot.put("updatedAtMs", System.currentTimeMillis());
            try { manifest.write(snapshot); }
            catch (IOException failure) { throw new UncheckedIOException("failed to persist query manifest", failure); }
        }
    }
}
