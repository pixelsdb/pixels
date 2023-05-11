package io.pixelsdb.pixels.worker.vhive;

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
}
