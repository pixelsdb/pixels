/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.worker.spike;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.S3QSStageWorkerRunner;
import io.pixelsdb.pixels.worker.common.WorkerContext;

public class S3QSPartitionedChainJoinWorker implements WorkerInterface<StageWorkerInput, JoinOutput>
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
        return this.context.getRequestId();
    }

    @Override
    public WorkerType getWorkerType()
    {
        return WorkerType.PARTITIONED_CHAIN_JOIN_S3QS;
    }
}
