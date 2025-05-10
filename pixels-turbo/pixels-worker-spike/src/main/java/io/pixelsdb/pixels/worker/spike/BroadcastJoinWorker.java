package io.pixelsdb.pixels.worker.spike;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.BaseBroadcastJoinWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;

public class BroadcastJoinWorker extends BaseBroadcastJoinWorker implements WorkerInterface<BroadcastJoinInput, JoinOutput>
{

    public BroadcastJoinWorker(WorkerContext context)
    {
        super(context);
    }

    @Override
    public JoinOutput handleRequest(BroadcastJoinInput input)
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
        return WorkerType.BROADCAST_JOIN;
    }
}