/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.planner.plan.physical;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.planner.coordinate.PlanCoordinator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author hank
 * @create 2022-07-05
 */
public abstract class Operator implements OperatorExecutor
{
    protected static final double StageCompletionRatio;
    protected static final ExecutorService operatorService = Executors.newCachedThreadPool();

    static
    {
        StageCompletionRatio = Double.parseDouble(
                ConfigFactory.Instance().getProperty("executor.stage.completion.ratio"));

        Runtime.getRuntime().addShutdownHook(new Thread(operatorService::shutdownNow));
    }

    private final String name;

    public Operator(String name)
    {
        this.name = name;
    }

    /**
     * @return the name of the operator
     */
    public String getName()
    {
        return name;
    }

    /**
     * Initialize the query plan coordinator. This method should be invoked recursively to traverse all
     * the operators in the query plan. Each operator added its own query execution stages into the plan
     * coordinator. Therefore, the users only need to call this method on the root operator of the plan.
     * @param planCoordinator the plan coordinator to be initialized
     * @param parentStageId the stage id of the parent (i.e., downstream) stage of this operator, for the
     *                      root operator, the parentStageId should be negative (e.g., -1), meaning the
     *                      parent does not exist for root
     */
    public abstract void initPlanCoordinator(PlanCoordinator planCoordinator, int parentStageId);
}
