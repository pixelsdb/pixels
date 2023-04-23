package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.BaseScanWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import org.slf4j.LoggerFactory;

public class ScanWorker extends BaseScanWorker implements RequestHandler<ScanInput, ScanOutput> {
    public ScanWorker() {
        super(new WorkerContext(LoggerFactory.getLogger(ScanWorker.class), new WorkerMetrics(), "myid"));
    }

    @Override
    public ScanOutput handleRequest(ScanInput event) {
        return process(event);
    }
}
