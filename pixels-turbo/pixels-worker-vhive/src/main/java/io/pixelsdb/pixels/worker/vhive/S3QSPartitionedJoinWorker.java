/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.S3QSStageWorkerRunner;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;

public class S3QSPartitionedJoinWorker implements RequestHandler<StageWorkerInput, JoinOutput>
{
    private final WorkerContext context;

    public S3QSPartitionedJoinWorker(WorkerContext context)
    {
        this.context = context;
    }

    @Override
    public JoinOutput handleRequest(StageWorkerInput input)
    {
        return new S3QSStageWorkerRunner(context).runPartitionedJoin(input);
    }

    @Override
    public String getRequestId()
    {
        return context.getRequestId();
    }

    @Override
    public WorkerType getWorkerType()
    {
        return WorkerType.PARTITIONED_JOIN_S3QS;
    }
}
