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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
    private final CoordinatorEndpoint coordinatorEndpoint;
    private final StageWorkerLauncher stageWorkerLauncher;
    /**
     * The assigner of stage id.
     */
    private final AtomicInteger stageIdAssigner = new AtomicInteger(0);

    public PlanCoordinator(long transId)
    {
        this(transId, CoordinatorEndpoint.fromConfig(), new InvokerStageWorkerLauncher());
    }

    public PlanCoordinator(long transId, StageWorkerLauncher stageWorkerLauncher)
    {
        this(transId, CoordinatorEndpoint.fromConfig(), stageWorkerLauncher);
    }

    public PlanCoordinator(long transId, CoordinatorEndpoint coordinatorEndpoint)
    {
        this(transId, coordinatorEndpoint, new InvokerStageWorkerLauncher());
    }

    public PlanCoordinator(long transId, CoordinatorEndpoint coordinatorEndpoint,
                           StageWorkerLauncher stageWorkerLauncher)
    {
        this.transId = transId;
        this.coordinatorEndpoint = requireNonNull(coordinatorEndpoint, "coordinatorEndpoint is null");
        this.stageWorkerLauncher = requireNonNull(stageWorkerLauncher, "stageWorkerLauncher is null");
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
        StageCoordinator stageCoordinator = requireNonNull(this.stageCoordinators.get(stageId),
                "stage coordinator is not found");
        return scaleStage(stageId, stageCoordinator.getPendingTaskCount());
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
     * Assign an id for a stage. This should only be called in
     * {@link io.pixelsdb.pixels.planner.plan.physical.Operator#initPlanCoordinator(PlanCoordinator, int, boolean)}
     * when building the plan coordinator for a query plan.
     * @return the assigned stage id
     */
    public int assignStageId()
    {
        return this.stageIdAssigner.getAndIncrement();
    }
}
