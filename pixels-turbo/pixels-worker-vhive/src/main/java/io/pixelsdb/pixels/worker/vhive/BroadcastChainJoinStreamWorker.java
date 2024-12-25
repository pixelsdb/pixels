package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.coordinate.CFWorkerInfo;
import io.pixelsdb.pixels.planner.coordinate.WorkerCoordinateService;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastChainJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.BaseBroadcastChainJoinWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class BroadcastChainJoinStreamWorker extends BaseBroadcastChainJoinWorker implements RequestHandler<BroadcastChainJoinInput, JoinOutput>
{
    private final Logger logger;
    protected WorkerCoordinateService workerCoordinatorService;
    private final WorkerMetrics workerMetrics;
    private io.pixelsdb.pixels.common.task.Worker<CFWorkerInfo> worker;
    private List<CFWorkerInfo> downStreamWorkers;

    public BroadcastChainJoinStreamWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
        this.workerMetrics.clear();
    }

    @Override
    public JoinOutput handleRequest(BroadcastChainJoinInput input)
    {
        logger.info("it's ok");
        return new JoinOutput();
    }

    @Override
    public String getRequestId() {
        return this.context.getRequestId();
    }

    @Override
    public WorkerType getWorkerType() {
        return WorkerType.BROADCAST_JOIN_STREAMING;
    }
}
