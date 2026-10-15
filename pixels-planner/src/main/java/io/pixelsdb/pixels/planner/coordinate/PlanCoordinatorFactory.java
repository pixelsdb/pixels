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

import io.pixelsdb.pixels.planner.plan.physical.Operator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * The factory to create and manage the plan coordinators of queries.
 * @author hank
 * @create 2023-09-25
 */
public class PlanCoordinatorFactory
{
    private static final class InstanceHolder
    {
        private static final PlanCoordinatorFactory instance = new PlanCoordinatorFactory();
    }

    public static PlanCoordinatorFactory Instance()
    {
        return InstanceHolder.instance;
    }

    private final Map<Long, PlanCoordinator> transIdToPlanCoordinator;

    private PlanCoordinatorFactory()
    {
        this.transIdToPlanCoordinator = new ConcurrentHashMap<>();
    }

    /**
     * Create the plan coordinator for the query plan.
     * @param transId the transaction id
     * @param planRootOperator the root operator of the query plan
     * @return the plan coordinator
     */
    public PlanCoordinator createPlanCoordinator(long transId, Operator planRootOperator)
    {
        return createPlanCoordinator(transId, planRootOperator, CoordinatorEndpoint.fromConfig());
    }

    public PlanCoordinator createPlanCoordinator(long transId, Operator planRootOperator,
                                                 CoordinatorEndpoint coordinatorEndpoint)
    {
        return createPlanCoordinator(transId, planRootOperator, coordinatorEndpoint,
                new InvokerStageWorkerLauncher());
    }

    public PlanCoordinator createPlanCoordinator(long transId, Operator planRootOperator,
                                                 CoordinatorEndpoint coordinatorEndpoint,
                                                 StageWorkerLauncher stageWorkerLauncher)
    {
        requireNonNull(planRootOperator, "planRootOperator is null");
        checkState(!this.transIdToPlanCoordinator.containsKey(transId),
                "plan coordinator already exists for transaction %s", transId);
        PlanCoordinator planCoordinator = new PlanCoordinator(transId, coordinatorEndpoint,
                requireNonNull(stageWorkerLauncher, "stageWorkerLauncher is null"));
        planRootOperator.initPlanCoordinator(planCoordinator, -1, false);
        PlanCoordinator previous = this.transIdToPlanCoordinator.putIfAbsent(transId, planCoordinator);
        checkState(previous == null, "plan coordinator already exists for transaction %s", transId);
        return planCoordinator;
    }

    public CoordinatedPlanExecution createPlanExecution(long transId, Operator planRootOperator)
    {
        return createPlanExecution(transId, planRootOperator, CoordinatorEndpoint.fromConfig());
    }

    public CoordinatedPlanExecution createPlanExecution(long transId, Operator planRootOperator,
                                                        CoordinatorEndpoint coordinatorEndpoint)
    {
        return createPlanExecution(transId, planRootOperator, coordinatorEndpoint,
                new S3QSShuffleResourceLifecycle());
    }

    public CoordinatedPlanExecution createPlanExecution(long transId, Operator planRootOperator,
                                                        CoordinatorEndpoint coordinatorEndpoint,
                                                        ShuffleResourceLifecycle shuffleResourceLifecycle)
    {
        return createPlanExecution(transId, planRootOperator, coordinatorEndpoint,
                shuffleResourceLifecycle, new InvokerStageWorkerLauncher());
    }

    public CoordinatedPlanExecution createPlanExecution(long transId, Operator planRootOperator,
                                                        CoordinatorEndpoint coordinatorEndpoint,
                                                        ShuffleResourceLifecycle shuffleResourceLifecycle,
                                                        StageWorkerLauncher stageWorkerLauncher)
    {
        PlanCoordinator planCoordinator =
                createPlanCoordinator(transId, planRootOperator, coordinatorEndpoint, stageWorkerLauncher);
        return new CoordinatedPlanExecution(transId, planRootOperator, planCoordinator, this,
                requireNonNull(shuffleResourceLifecycle, "shuffleResourceLifecycle is null"));
    }

    /**
     * Retrieve the plan coordinator of the query.
     * @param transId the transaction id of the query
     * @return the plan coordinator
     */
    public PlanCoordinator getPlanCoordinator(long transId)
    {
        return this.transIdToPlanCoordinator.get(transId);
    }

    /**
     * Remove only the coordinator owned by the given execution. This prevents
     * a stale execution from deleting a newer coordinator with the same id.
     */
    public boolean removePlanCoordinator(long transId, PlanCoordinator planCoordinator)
    {
        return this.transIdToPlanCoordinator.remove(transId,
                requireNonNull(planCoordinator, "planCoordinator is null"));
    }
}
