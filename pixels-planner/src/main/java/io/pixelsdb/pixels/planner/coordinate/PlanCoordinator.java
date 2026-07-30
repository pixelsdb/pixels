/*
 * Copyright 2023 PixelsDB.
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
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleQueueInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The coordinator of a query plan.
 * @author hank
 * @create 2023-07-31
 */
public class PlanCoordinator
{
    /**
     * The transaction id of the query.
     */
    private final long transId;
    /**
     * Mapping from stage id to stage coordinator.
     * It is only modified during coordinator initialization, thus there is no read-write conflict.
     */
    private final Map<Integer, StageCoordinator> stageCoordinators = new HashMap<>();
    /**
     * Mapping from stage id to stage dependency.
     * It is only modified during coordinator initialization, thus there is no read-write conflict.
     */
    private final Map<Integer, StageDependency> stageDependencies = new HashMap<>();
    /**
     * Runtime worker control is registered only for stages whose physical
     * workers pull coordinator tasks, currently the S3QS shuffle stages.
     */
    private final Map<Integer, StageRuntimeController> stageRuntimeControllers = new HashMap<>();
    private final Map<Integer, StageAutoScaler> stageAutoScalers = new HashMap<>();
    /**
     * Query-owned shuffle edges keyed by their query-unique shuffle id.
     */
    private final Map<String, ShuffleInfo> shuffleInfos = new HashMap<>();
    private final CoordinatorEndpoint coordinatorEndpoint;
    private final StageWorkerLauncher stageWorkerLauncher;
    private final StageScalingConfig stageScalingConfig;
    private ScheduledExecutorService scalingExecutor;
    /**
     * The assigner of stage id.
     */
    private final AtomicInteger stageIdAssigner = new AtomicInteger(0);

    public PlanCoordinator(long transId)
    {
        this(transId, CoordinatorEndpoint.fromConfig(), new InvokerStageWorkerLauncher(),
                StageScalingConfig.fromConfig());
    }

    public PlanCoordinator(long transId, StageWorkerLauncher stageWorkerLauncher)
    {
        this(transId, CoordinatorEndpoint.fromConfig(), stageWorkerLauncher,
                StageScalingConfig.fromConfig());
    }

    public PlanCoordinator(long transId, CoordinatorEndpoint coordinatorEndpoint)
    {
        this(transId, coordinatorEndpoint, new InvokerStageWorkerLauncher(),
                StageScalingConfig.fromConfig());
    }

    public PlanCoordinator(long transId, CoordinatorEndpoint coordinatorEndpoint,
                           StageWorkerLauncher stageWorkerLauncher)
    {
        this(transId, coordinatorEndpoint, stageWorkerLauncher, StageScalingConfig.fromConfig());
    }

    public PlanCoordinator(long transId, CoordinatorEndpoint coordinatorEndpoint,
                           StageWorkerLauncher stageWorkerLauncher,
                           StageScalingConfig stageScalingConfig)
    {
        this.transId = transId;
        this.coordinatorEndpoint = requireNonNull(coordinatorEndpoint, "coordinatorEndpoint is null");
        this.stageWorkerLauncher = requireNonNull(stageWorkerLauncher, "stageWorkerLauncher is null");
        this.stageScalingConfig = requireNonNull(stageScalingConfig, "stageScalingConfig is null");
    }

    /**
     * Add a new stage and its downstream dependency to this plan coordinator. All the stages must be added in the order
     * of downstream to up stream, i.e., from the root operator to leaf operators.
     * @param stageCoordinator the stage coordinator of the current stage
     * @param stageDependency the downstream dependency of the current stage, for the root operator, the down stream
     *                       dependency is empty (with downstream stage id < 0)
     */
    public void addStageCoordinator(StageCoordinator stageCoordinator, StageDependency stageDependency)
    {
        int stageId = requireNonNull(stageCoordinator, "stageCoordinator is null").getStageId();
        requireNonNull(stageDependency, "stageDependency is null");
        checkArgument(!this.stageCoordinators.containsKey(stageId), "stageId already exists");
        checkArgument(stageCoordinator.getStageId() == stageDependency.getCurrentStageId(),
                "the stageDependency does not belong to the stageCoordinator");
        this.stageCoordinators.put(stageId, stageCoordinator);
        if (stageDependency.getDownStreamStageId() != -1)
        {
            StageCoordinator parentStageCoordinator = this.stageCoordinators.get(stageDependency.getDownStreamStageId());
            stageCoordinator.setDownStreamWorkerNum(parentStageCoordinator.getFixedWorkerNum());
        }
        this.stageDependencies.put(stageId, stageDependency);
    }

    public StageCoordinator getStageCoordinator(int stageId)
    {
        return this.stageCoordinators.get(stageId);
    }

    /**
     * Register the physical execution description for a queued stage. Logical
     * tasks must already exist in the corresponding StageCoordinator.
     */
    public void addStageRuntimeController(StageExecutionDescriptor descriptor)
    {
        requireNonNull(descriptor, "descriptor is null");
        int stageId = descriptor.getStageId();
        StageCoordinator stageCoordinator = requireNonNull(this.stageCoordinators.get(stageId),
                "stage coordinator is not found");
        checkArgument(!this.stageRuntimeControllers.containsKey(stageId),
                "stage runtime controller already exists");
        // Runtime S3QS workers claim one task at a time. This keeps unstarted
        // work visible to the autoscaler and prevents one worker from
        // reserving the whole logical task queue.
        stageCoordinator.setTaskBatchSize(1);
        this.stageRuntimeControllers.put(stageId,
                new StageRuntimeController(stageCoordinator, descriptor, this.stageWorkerLauncher));
    }

    public StageRuntimeController getStageRuntimeController(int stageId)
    {
        return this.stageRuntimeControllers.get(stageId);
    }

    /**
     * Start the first worker set for a queued stage. The initial target is
     * bounded by its current pending logical task count.
     */
    public List<CompletableFuture<? extends Output>> activateStage(int stageId)
    {
        return activateStageExecution(stageId).getAttemptFutures();
    }

    /**
     * Activate a queued stage and return a stable handle that also observes
     * worker attempts launched by later scaling decisions.
     */
    public synchronized StageExecutionHandle activateStageExecution(int stageId)
    {
        StageRuntimeController runtimeController = requireNonNull(
                this.stageRuntimeControllers.get(stageId), "stage runtime controller is not found");
        StageExecutionHandle handle = runtimeController.getExecutionHandle();
        if (!handle.getAttemptFutures().isEmpty() || this.stageAutoScalers.containsKey(stageId))
        {
            return handle;
        }

        StageCoordinator stageCoordinator = requireNonNull(this.stageCoordinators.get(stageId),
                "stage coordinator is not found");
        int pendingTasks = stageCoordinator.getPendingTaskCount();
        if (stageScalingConfig.isEnabled())
        {
            int initialTarget = Math.min(pendingTasks,
                    Math.min(stageScalingConfig.getMaxWorkers(),
                            Math.max(stageScalingConfig.getMinWorkers(),
                                    stageScalingConfig.getInitialWorkers())));
            runtimeController.scaleTo(initialTarget);
            StageAutoScaler autoScaler = new StageAutoScaler(runtimeController,
                    new PendingTaskScalingPolicy(), stageScalingConfig);
            this.stageAutoScalers.put(stageId, autoScaler);
            autoScaler.start(getOrCreateScalingExecutor());
            handle.getCompletionFuture().whenComplete((outputs, failure) -> stopStageAutoScaler(stageId));
        }
        else
        {
            runtimeController.scaleTo(pendingTasks);
        }
        return handle;
    }

    /**
     * Manually change the desired runtime worker count for a stage.
     */
    public List<CompletableFuture<? extends Output>> scaleStage(int stageId, int targetWorkerCount)
    {
        StageRuntimeController runtimeController = requireNonNull(this.stageRuntimeControllers.get(stageId),
                "stage runtime controller is not found");
        return runtimeController.scaleTo(targetWorkerCount);
    }

    public StageExecutionHandle getStageExecutionHandle(int stageId)
    {
        StageRuntimeController runtimeController = this.stageRuntimeControllers.get(stageId);
        return runtimeController == null ? null : runtimeController.getExecutionHandle();
    }

    StageScalingDecision runStageScalingOnce(int stageId, long currentTimeMs)
    {
        StageAutoScaler autoScaler;
        synchronized (this)
        {
            autoScaler = requireNonNull(this.stageAutoScalers.get(stageId),
                    "stage auto scaler is not active");
        }
        return autoScaler.runOnce(currentTimeMs);
    }

    public synchronized void close()
    {
        for (StageAutoScaler autoScaler : this.stageAutoScalers.values())
        {
            autoScaler.close();
        }
        this.stageAutoScalers.clear();
        if (this.scalingExecutor != null)
        {
            this.scalingExecutor.shutdownNow();
            this.scalingExecutor = null;
        }
    }

    /**
     * Get this stage's dependency on the parent (downstream) stage.
     * @param stageId the id of this stage
     * @return the dependency
     */
    public StageDependency getStageDependency(int stageId)
    {
        return this.stageDependencies.get(stageId);
    }

    /**
     * @return the transaction id of this query
     */
    public long getTransId()
    {
        return transId;
    }

    public CoordinatorEndpoint getCoordinatorEndpoint()
    {
        return coordinatorEndpoint;
    }

    /**
     * Register one shuffle edge with this query. Producer and consumer inputs
     * may reference separate but equivalent metadata objects, so duplicates
     * are accepted only when all resource-defining fields agree.
     */
    public void addShuffleInfo(ShuffleInfo shuffleInfo)
    {
        requireNonNull(shuffleInfo, "shuffleInfo is null");
        String shuffleId = requireNonNull(shuffleInfo.getShuffleId(), "shuffleId is null");
        checkArgument(!shuffleId.trim().isEmpty(), "shuffleId is empty");
        ShuffleInfo previous = this.shuffleInfos.get(shuffleId);
        if (previous == null)
        {
            this.shuffleInfos.put(shuffleId, shuffleInfo);
            return;
        }
        checkArgument(sameShuffleResource(previous, shuffleInfo),
                "conflicting shuffle metadata for shuffleId " + shuffleId);
    }

    public Collection<ShuffleInfo> getShuffleInfos()
    {
        return Collections.unmodifiableCollection(new ArrayList<>(this.shuffleInfos.values()));
    }

    /**
     * Assign an id for a stage. This should only be called in
     * {@link io.pixelsdb.pixels.planner.plan.physical.Operator#initPlanCoordinator(PlanCoordinator, int, boolean)}
     * when building the plan coordinator for a query plan.
     * @return the assigned stage id
     */
    public int assignStageId()
    {
        return this.stageIdAssigner.getAndIncrement();
    }

    private synchronized ScheduledExecutorService getOrCreateScalingExecutor()
    {
        if (this.scalingExecutor == null)
        {
            this.scalingExecutor = Executors.newSingleThreadScheduledExecutor(runnable ->
            {
                Thread thread = new Thread(runnable, "s3qs-stage-scaler-" + transId);
                thread.setDaemon(true);
                return thread;
            });
        }
        return this.scalingExecutor;
    }

    private synchronized void stopStageAutoScaler(int stageId)
    {
        StageAutoScaler autoScaler = this.stageAutoScalers.remove(stageId);
        if (autoScaler != null)
        {
            autoScaler.close();
        }
    }

    private static boolean sameShuffleResource(ShuffleInfo left, ShuffleInfo right)
    {
        if (!Objects.equals(left.getObjectPathPrefix(), right.getObjectPathPrefix()) ||
                left.getNumPartitions() != right.getNumPartitions() ||
                left.getStorageInfo() == null || right.getStorageInfo() == null ||
                left.getStorageInfo().getScheme() != right.getStorageInfo().getScheme())
        {
            return false;
        }
        List<ShuffleQueueInfo> leftQueues = left.getQueues();
        List<ShuffleQueueInfo> rightQueues = right.getQueues();
        if (leftQueues == null || rightQueues == null || leftQueues.size() != rightQueues.size())
        {
            return false;
        }
        Map<Integer, ShuffleQueueInfo> rightByPartition = new HashMap<>();
        for (ShuffleQueueInfo queue : rightQueues)
        {
            rightByPartition.put(queue.getPartitionId(), queue);
        }
        for (ShuffleQueueInfo leftQueue : leftQueues)
        {
            ShuffleQueueInfo rightQueue = rightByPartition.get(leftQueue.getPartitionId());
            if (rightQueue == null ||
                    !Objects.equals(leftQueue.getQueueName(), rightQueue.getQueueName()) ||
                    !Objects.equals(leftQueue.getQueueUrl(), rightQueue.getQueueUrl()))
            {
                return false;
            }
        }
        return true;
    }
}
