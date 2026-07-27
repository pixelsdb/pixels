/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public License
 * along with Pixels.  If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.Operator;
import io.pixelsdb.pixels.planner.plan.physical.OperatorExecutor.OutputCollection;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Owns coordinator registration for one physical query-plan execution.
 *
 * Callers should use try-with-resources so exceptional execution paths also
 * remove the query coordinator. Normal output collection closes it
 * automatically after the operator tree has completed.
 */
public class CoordinatedPlanExecution implements AutoCloseable
{
    private final long transId;
    private final Operator rootOperator;
    private final PlanCoordinator planCoordinator;
    private final PlanCoordinatorFactory coordinatorFactory;
    private CompletableFuture<CompletableFuture<? extends Output>[]> executionFuture;
    private boolean executed;
    private boolean closed;

    CoordinatedPlanExecution(long transId, Operator rootOperator, PlanCoordinator planCoordinator,
                             PlanCoordinatorFactory coordinatorFactory)
    {
        this.transId = transId;
        this.rootOperator = requireNonNull(rootOperator, "rootOperator is null");
        this.planCoordinator = requireNonNull(planCoordinator, "planCoordinator is null");
        this.coordinatorFactory = requireNonNull(coordinatorFactory, "coordinatorFactory is null");
    }

    public synchronized CompletableFuture<CompletableFuture<? extends Output>[]> execute()
    {
        checkState(!closed, "plan execution is closed");
        checkState(!executed, "plan execution has already started");
        executed = true;
        executionFuture = rootOperator.execute();
        return executionFuture;
    }

    public synchronized OutputCollection collectOutputs() throws ExecutionException, InterruptedException
    {
        checkState(!closed, "plan execution is closed");
        checkState(executed, "plan execution has not started");
        try
        {
            // Ensure the final stage has been started and its worker futures
            // have been installed before traversing the operator output tree.
            executionFuture.get();
            return rootOperator.collectOutputs();
        }
        finally
        {
            close();
        }
    }

    public PlanCoordinator getPlanCoordinator()
    {
        return planCoordinator;
    }

    @Override
    public synchronized void close()
    {
        if (!closed)
        {
            coordinatorFactory.removePlanCoordinator(transId, planCoordinator);
            closed = true;
        }
    }
}
