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

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;

import static java.util.Objects.requireNonNull;

/**
 * Immutable information required to start a worker for one queued stage.
 *
 * Logical task payloads are deliberately absent: workers use this bootstrap
 * information to register and pull those payloads from StageCoordinator.
 */
public class StageExecutionDescriptor
{
    private final long transId;
    private final long timestamp;
    private final int stageId;
    private final String operatorName;
    private final WorkerType workerType;
    private final CoordinatorEndpoint coordinatorEndpoint;

    public StageExecutionDescriptor(long transId, long timestamp, int stageId,
                                    String operatorName, WorkerType workerType,
                                    CoordinatorEndpoint coordinatorEndpoint)
    {
        this.transId = transId;
        this.timestamp = timestamp;
        this.stageId = stageId;
        this.operatorName = requireNonNull(operatorName, "operatorName is null");
        this.workerType = requireNonNull(workerType, "workerType is null");
        this.coordinatorEndpoint = requireNonNull(coordinatorEndpoint, "coordinatorEndpoint is null");
    }

    public StageWorkerInput createWorkerInput()
    {
        StageWorkerInput input = new StageWorkerInput(transId, timestamp, stageId, operatorName, workerType);
        input.setCoordinatorHost(coordinatorEndpoint.getHost());
        input.setCoordinatorPort(coordinatorEndpoint.getPort());
        return input;
    }

    public int getStageId()
    {
        return stageId;
    }

    public WorkerType getWorkerType()
    {
        return workerType;
    }
}
