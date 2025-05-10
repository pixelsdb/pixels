package io.pixelsdb.pixels.worker.spike;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.BasePartitionedJoinWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;

public class PartitionedJoinWorker extends BasePartitionedJoinWorker implements WorkerInterface<PartitionedJoinInput, JoinOutput>
{
    public PartitionedJoinWorker(WorkerContext context)
    {
        super(context);
    }

    @Override
    public JoinOutput handleRequest(PartitionedJoinInput input)
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
        return WorkerType.PARTITIONED_JOIN;
    }
}
