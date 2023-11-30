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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2023-07-31
 */
public class PlanCoordinator
{
    private final long transId;
    /**
     * It is only modified during coordinator initialization, thus there is no read-write conflict.
     */
    private final Map<Integer, StageCoordinator> stageCoordinators = new HashMap<>();
    /**
     * It is only modified during coordinator initialization, thus there is no read-write conflict.
     */
    private final Map<Integer, StageDependency> stageDependencies = new HashMap<>();
    /**
     * The assigner of stage id.
     */
    private final AtomicInteger stageIdAssigner = new AtomicInteger(0);

    public PlanCoordinator(long transId)
    {
        this.transId = transId;
    }

    public void addStageCoordinator(StageCoordinator stageCoordinator, StageDependency stageDependency)
    {
        int stageId = requireNonNull(stageCoordinator, "stageCoordinator is null").getStageId();
        requireNonNull(stageDependency, "stageDependency is null");
        checkArgument(!this.stageCoordinators.containsKey(stageId), "stageId already exists");
        checkArgument(stageCoordinator.getStageId() == stageDependency.getCurrentStageId(),
                "the stageDependency does not belong to the stageCoordinator");
        this.stageCoordinators.put(stageId, stageCoordinator);
        this.stageDependencies.put(stageId, stageDependency);
    }

    public StageCoordinator getStageCoordinator(int stageId)
    {
        return this.stageCoordinators.get(stageId);
    }

    public StageDependency getStageDependency(int stageId)
    {
        return this.stageDependencies.get(stageId);
    }

    public long getTransId()
    {
        return transId;
    }

    public int assignStageId()
    {
        return this.stageIdAssigner.getAndIncrement();
    }
}
