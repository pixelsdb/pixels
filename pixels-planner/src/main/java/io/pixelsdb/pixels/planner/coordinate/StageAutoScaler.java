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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Periodically turns immutable stage status snapshots into capacity targets.
 */
public class StageAutoScaler implements AutoCloseable
{
    private static final Logger log = LogManager.getLogger(StageAutoScaler.class);

    private final StageRuntimeController runtimeController;
    private final StageScalingPolicy policy;
    private final StageScalingConfig config;
    private final StageScalingHistory history = new StageScalingHistory();
    private volatile ScheduledFuture<?> scheduledFuture;
    private volatile boolean closed;

    public StageAutoScaler(StageRuntimeController runtimeController,
                           StageScalingPolicy policy, StageScalingConfig config)
    {
        this.runtimeController = requireNonNull(runtimeController, "runtimeController is null");
        this.policy = requireNonNull(policy, "policy is null");
        this.config = requireNonNull(config, "config is null");
    }

    public synchronized void start(ScheduledExecutorService executor)
    {
        if (closed || scheduledFuture != null)
        {
            return;
        }
        long periodMs = config.getSamplePeriodMs();
        scheduledFuture = requireNonNull(executor, "executor is null").scheduleWithFixedDelay(
                this::runSafely, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Execute one deterministic sample. Tests use this method with controlled
     * task states; production scheduling calls the same path.
     */
    public synchronized StageScalingDecision runOnce(long currentTimeMs)
    {
        StageRuntimeStatus status = runtimeController.getStatus();
        if (status.isLogicalTasksTerminal())
        {
            runtimeController.refreshCompletion();
            close();
            return new StageScalingDecision(0, StageScalingDecision.Direction.SCALE_IN,
                    "stage is terminal");
        }
        StageScalingDecision decision = policy.evaluate(status, history, config, currentTimeMs);
        log.info("s3qs_scaling_decision stageId={} direction={} desired={} target={} pending={} running={} accepting={} draining={} reason={}",
                runtimeController.getStageId(), decision.getDirection(), status.getDesiredWorkerCount(),
                decision.getTargetWorkerCount(), status.getPendingTaskCount(), status.getRunningTaskCount(),
                status.getAcceptingWorkerCount(), status.getDrainingWorkerCount(), decision.getReason());
        if (decision.getTargetWorkerCount() != status.getDesiredWorkerCount())
        {
            runtimeController.scaleTo(decision.getTargetWorkerCount());
        }
        return decision;
    }

    @Override
    public void close()
    {
        closed = true;
        ScheduledFuture<?> scheduled = scheduledFuture;
        if (scheduled != null)
        {
            scheduled.cancel(false);
            scheduledFuture = null;
        }
    }

    public synchronized boolean isClosed()
    {
        return closed;
    }

    private void runSafely()
    {
        try
        {
            runOnce(System.currentTimeMillis());
        }
        catch (Throwable e)
        {
            log.error("failed to evaluate S3QS stage scaling for stage " +
                    runtimeController.getStageId(), e);
        }
    }
}
