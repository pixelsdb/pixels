package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.BaseBroadcastJoinWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;

public class BroadcastJoinWorker extends BaseBroadcastJoinWorker implements RequestHandler<BroadcastJoinInput, JoinOutput> {

    public BroadcastJoinWorker(WorkerContext context) {
        super(context);
    }

    @Override
    public JoinOutput handleRequest(BroadcastJoinInput input) {
        return process(input);
    }
}
