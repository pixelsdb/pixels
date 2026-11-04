/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.S3QSStageWorkerRunner;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;

public class S3QSPartitionedChainJoinWorker implements RequestHandler<StageWorkerInput, JoinOutput>
{
    private final WorkerContext context;

    public S3QSPartitionedChainJoinWorker(WorkerContext context)
    {
        this.context = context;
    }

    @Override
    public JoinOutput handleRequest(StageWorkerInput input)
    {
        return new S3QSStageWorkerRunner(context).runPartitionedChainJoin(input);
    }

    @Override
    public String getRequestId()
    {
        return context.getRequestId();
    }

    @Override
    public WorkerType getWorkerType()
    {
        return WorkerType.PARTITIONED_CHAIN_JOIN_S3QS;
    }
}
