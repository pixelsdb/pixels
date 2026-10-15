/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.StageWorkerInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.worker.common.S3QSStageWorkerRunner;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;

public class S3QSPartitionWorker implements RequestHandler<StageWorkerInput, PartitionOutput>
{
    private final WorkerContext context;

    public S3QSPartitionWorker(WorkerContext context)
    {
        this.context = context;
    }

    @Override
    public PartitionOutput handleRequest(StageWorkerInput input)
    {
        return new S3QSStageWorkerRunner(context).runPartition(input);
    }

    @Override
    public String getRequestId()
    {
        return context.getRequestId();
    }

    @Override
    public WorkerType getWorkerType()
    {
        return WorkerType.PARTITION_S3QS;
    }
}
