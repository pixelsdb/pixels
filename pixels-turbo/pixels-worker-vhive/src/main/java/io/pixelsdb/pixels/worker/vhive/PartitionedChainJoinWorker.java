package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedChainJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.BasePartitionedChainJoinWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;

public class PartitionedChainJoinWorker extends BasePartitionedChainJoinWorker implements RequestHandler<PartitionedChainJoinInput, JoinOutput> {
    public PartitionedChainJoinWorker(WorkerContext context) {
        super(context);
    }

    @Override
    public JoinOutput handleRequest(PartitionedChainJoinInput input) {
        return process(input);
    }
}
