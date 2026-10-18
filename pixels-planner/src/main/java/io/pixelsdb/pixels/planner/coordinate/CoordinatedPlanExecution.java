/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.Operator;
import io.pixelsdb.pixels.planner.plan.physical.OperatorExecutor.OutputCollection;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/** Query result deadlines and remote quiescence are independent lifecycles. */
public class CoordinatedPlanExecution implements AutoCloseable
{
    public enum CleanupStatus { NOT_STARTED, WAITING_FOR_QUIESCENCE, CLEANED, CLEANUP_DEFERRED, CLEANUP_FAILED }

    private final long transId;
    private final Operator rootOperator;
    private final PlanCoordinator planCoordinator;
    private final PlanCoordinatorFactory coordinatorFactory;
    private final ShuffleResourceLifecycle shuffleResourceLifecycle;
    private final QueryExecutionConfig config;
    private final QueryCleanupManifest manifest;
    private final ExecutorService executor;
    private final ScheduledExecutorService timer;
    private final CompletableFuture<CompletableFuture<? extends Output>[]> executionFuture = new CompletableFuture<>();
    private final CompletableFuture<OutputCollection> collected = new CompletableFuture<>();
    private final CompletableFuture<CleanupStatus> cleanupCompletion = new CompletableFuture<>();
    private final AtomicBoolean collecting = new AtomicBoolean();
    private final AtomicBoolean cleanupStarted = new AtomicBoolean();
    private volatile CleanupStatus cleanupStatus = CleanupStatus.NOT_STARTED;
    private volatile Map<String, Object> resources = Collections.emptyMap();
    private volatile Throwable cleanupFailure;
    private boolean executed;
    private volatile boolean preparationFinished = true;
    private volatile boolean resourcesPrepared;
    private volatile boolean closed;
    private volatile Future<?> startupTask;
    private volatile Future<?> collectionTask;

    CoordinatedPlanExecution(long transId, Operator rootOperator, PlanCoordinator planCoordinator,
                             PlanCoordinatorFactory coordinatorFactory, ShuffleResourceLifecycle lifecycle)
    {
        this(transId, rootOperator, planCoordinator, coordinatorFactory, lifecycle, QueryExecutionConfig.fromConfig());
    }

    CoordinatedPlanExecution(long transId, Operator rootOperator, PlanCoordinator planCoordinator,
                             PlanCoordinatorFactory coordinatorFactory, ShuffleResourceLifecycle lifecycle,
                             QueryExecutionConfig config)
    {
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
        planCoordinator.getQueryState().getFailureFuture().whenComplete((ignored, error) ->
        {
            Throwable cause = planCoordinator.getQueryState().getFirstFailure();
            executionFuture.completeExceptionally(cause);
            collected.completeExceptionally(cause);
            Future<?> startup = startupTask;
            if (startup != null) { startup.cancel(true); }
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
            if (planCoordinator.getQueryState().getStatus() == QueryExecutionState.Status.ABORTED)
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
        try
        {
            persistManifest();
            planCoordinator.getQueryState().requireRunning();
            resourcesPrepared = true;
            shuffleResourceLifecycle.prepare(planCoordinator.getShuffleInfos());
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
        finally { preparationFinished = true; }
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
            CleanupStatus status = cleanupCompletion.get(config.abortGraceMs + config.cleanupTimeoutMs + 1000,
                    TimeUnit.MILLISECONDS);
            if (status != CleanupStatus.CLEANED)
            {
                throw new ExecutionException("query completed but resource cleanup is " + status, cleanupFailure);
            }
            return result;
        }
        catch (TimeoutException failure) { throw new ExecutionException("cleanup did not finish within budget", failure); }
        catch (InterruptedException failure)
        {
            abort(failure);
            Thread.currentThread().interrupt();
            throw failure;
        }
    }

    public boolean abort(Throwable failure) { return planCoordinator.getQueryState().abort(failure); }
    public PlanCoordinator getPlanCoordinator() { return planCoordinator; }
    public CleanupStatus getCleanupStatus() { return cleanupStatus; }
    public CompletableFuture<CleanupStatus> getCleanupCompletion() { return cleanupCompletion; }
    public Path getManifestPath() { return manifest.getPath(); }
    public Throwable getCleanupFailure() { return cleanupFailure; }

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
        if (preparationFinished && planCoordinator.getQueryState().isQuiescent()) { finishCleanup(); }
        else
        {
            cleanupStatus = CleanupStatus.WAITING_FOR_QUIESCENCE;
            persistManifest();
            executor.execute(() ->
            {
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.abortGraceMs);
                try
                {
                    while ((!preparationFinished || !planCoordinator.getQueryState().isQuiescent()) &&
                            System.nanoTime() < deadline)
                    {
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                    if (preparationFinished && planCoordinator.getQueryState().isQuiescent()) { finishCleanup(); }
                    else
                    {
                        cleanupStatus = CleanupStatus.CLEANUP_DEFERRED;
                        persistManifest();
                        coordinatorFactory.retainTerminalCoordinator(transId, planCoordinator);
                        cleanupCompletion.complete(cleanupStatus);
                        executor.shutdown();
                    }
                }
                catch (Throwable failure) { recordCleanupFailure(failure); }
            });
        }
    }

    private void finishCleanup()
    {
        if (!cleanupStarted.compareAndSet(false, true)) { return; }
        Future<?> cleanup = executor.submit(() ->
        {
            try { if (resourcesPrepared) { shuffleResourceLifecycle.cleanup(); } }
            catch (IOException failure) { throw new UncheckedIOException(failure); }
        });
        try
        {
            cleanup.get(config.cleanupTimeoutMs, TimeUnit.MILLISECONDS);
            cleanupStatus = CleanupStatus.CLEANED;
            persistManifest();
            coordinatorFactory.removePlanCoordinator(transId, planCoordinator);
            cleanupCompletion.complete(cleanupStatus);
            executor.shutdown();
        }
        catch (Throwable failure) { recordCleanupFailure(QueryExecutionState.unwrap(failure)); }
    }

    private void closeAfterFailure()
    {
        try { close(); }
        catch (Throwable failure) { recordCleanupFailure(failure); }
    }

    private void recordCleanupFailure(Throwable failure)
    {
        cleanupFailure = failure;
        cleanupStatus = CleanupStatus.CLEANUP_FAILED;
        try { persistManifest(); }
        catch (RuntimeException manifestFailure) { failure.addSuppressed(manifestFailure); }
        coordinatorFactory.retainTerminalCoordinator(transId, planCoordinator);
        cleanupCompletion.complete(cleanupStatus);
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
