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
package io.pixelsdb.pixels.planner.plan.physical.input;

import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.WorkerType;

/**
 * Bootstrap input for a coordinator-dispatched stage worker.
 *
 * The real work item is not embedded in this input. The remote worker uses
 * transId and stageId to register with the coordinator and pulls task payloads
 * from the stage task queue.
 */
public class StageWorkerInput extends Input
{
    private String coordinatorHost = "128.110.218.225";
    private int coordinatorPort = 18894;
    private WorkerType workerType;

    public StageWorkerInput()
    {
        super(0L, 0L);
    }

    public StageWorkerInput(long transId, long timestamp, int stageId, String operatorName, WorkerType workerType)
    {
        super(transId, timestamp);
        setStageId(stageId);
        setOperatorName(operatorName);
        this.workerType = workerType;
    }

    public WorkerType getWorkerType()
    {
        return workerType;
    }

    public void setWorkerType(WorkerType workerType)
    {
        this.workerType = workerType;
    }

    public String getCoordinatorHost()
    {
        return coordinatorHost;
    }

    public void setCoordinatorHost(String coordinatorHost)
    {
        this.coordinatorHost = coordinatorHost;
    }

    public int getCoordinatorPort()
    {
        return coordinatorPort;
    }

    public void setCoordinatorPort(int coordinatorPort)
    {
        this.coordinatorPort = coordinatorPort;
    }
}
