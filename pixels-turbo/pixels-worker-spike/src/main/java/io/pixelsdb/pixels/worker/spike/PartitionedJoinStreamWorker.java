package io.pixelsdb.pixels.worker.spike;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.BasePartitionedJoinStreamWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;

public class PartitionedJoinStreamWorker extends BasePartitionedJoinStreamWorker implements WorkerInterface<PartitionedJoinInput, JoinOutput>
{
    public PartitionedJoinStreamWorker(WorkerContext context)
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
        return WorkerType.PARTITIONED_JOIN_STREAMING;
    }
}
