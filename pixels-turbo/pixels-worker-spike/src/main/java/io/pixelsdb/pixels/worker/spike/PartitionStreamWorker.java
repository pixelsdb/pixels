package io.pixelsdb.pixels.worker.spike;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.worker.common.BasePartitionStreamWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;

public class PartitionStreamWorker extends BasePartitionStreamWorker implements WorkerInterface<PartitionInput, PartitionOutput>
{
    public PartitionStreamWorker(WorkerContext context)
    {
        super(context);
    }

    @Override
    public PartitionOutput handleRequest(PartitionInput input)
    {
        return process(input);
    }

    @Override
    public String getRequestId()
    {
        return this.context.getRequestId();
    }

    @Override
    public WorkerType getWorkerType()
    {
        return WorkerType.PARTITION_STREAMING;
    }
}

