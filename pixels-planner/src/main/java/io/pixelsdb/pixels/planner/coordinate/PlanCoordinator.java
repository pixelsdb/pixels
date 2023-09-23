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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2023-07-31
 */
public class PlanCoordinator
{
    private final Map<Integer, StageCoordinator> stageCoordinators;

    public PlanCoordinator()
    {
        this.stageCoordinators = new ConcurrentHashMap<>();
    }

    public void addStageCoordinator(int stageId, StageCoordinator stageCoordinator)
    {
        checkArgument(!this.stageCoordinators.containsKey(stageId), "stageId already exists");
        this.stageCoordinators.put(stageId, requireNonNull(stageCoordinator, "stageCoordinator is null"));
    }

    public StageCoordinator getStageCoordinator(int stageId)
    {
        return this.stageCoordinators.get(stageId);
    }
}
