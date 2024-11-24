package io.pixelsdb.pixels.worker.spike;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.worker.common.BasePartitionWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;

public class PartitionWorker  extends BasePartitionWorker implements WorkerInterface<PartitionInput, PartitionOutput>
{
    public PartitionWorker(WorkerContext context)
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
        return WorkerType.PARTITION;
    }
}
