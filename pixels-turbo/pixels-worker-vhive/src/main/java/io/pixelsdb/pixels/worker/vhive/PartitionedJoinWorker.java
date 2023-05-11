package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.BasePartitionedJoinWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;

public class PartitionedJoinWorker extends BasePartitionedJoinWorker implements RequestHandler<PartitionedJoinInput, JoinOutput> {
    public PartitionedJoinWorker(WorkerContext context) {
        super(context);
    }

    @Override
    public JoinOutput handleRequest(PartitionedJoinInput input) {
        return process(input);
    }
}
