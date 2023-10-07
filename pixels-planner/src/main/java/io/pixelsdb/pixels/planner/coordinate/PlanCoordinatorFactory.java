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

/**
 * @author hank
 * @create 2023-09-25
 */
public class PlanCoordinatorFactory
{
    private static final PlanCoordinatorFactory instance = new PlanCoordinatorFactory();

    public static PlanCoordinatorFactory Instance()
    {
        return instance;
    }

    private final Map<Long, PlanCoordinator> transIdToPlanCoordinator;

    private PlanCoordinatorFactory()
    {
        this.transIdToPlanCoordinator = new ConcurrentHashMap<>();
    }

    public void createPlanCoordinator(long transId, Operator planRootOperator)
    {
        PlanCoordinator planCoordinator = new PlanCoordinator(transId);
        planRootOperator.initPlanCoordinator(planCoordinator, -1);
        this.transIdToPlanCoordinator.put(transId, planCoordinator);
    }

    public PlanCoordinator getPlanCoordinator(long transId)
    {
        return this.transIdToPlanCoordinator.get(transId);
    }
}
