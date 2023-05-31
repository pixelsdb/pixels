package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastChainJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.BaseBroadcastChainJoinWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;

public class BroadcastChainJoinWorker extends BaseBroadcastChainJoinWorker implements RequestHandler<BroadcastChainJoinInput, JoinOutput> {
    public BroadcastChainJoinWorker(WorkerContext context) {
        super(context);
    }

    @Override
    public JoinOutput handleRequest(BroadcastChainJoinInput input) {
        return process(input);
    }

    @Override
    public String getRequestId() {
        return this.context.getRequestId();
    }

    @Override
    public WorkerType getWorkerType() {
        return WorkerType.BROADCAST_CHAIN_JOIN;
    }
}
