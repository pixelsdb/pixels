package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.BaseScanWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;

public class ScanWorker extends BaseScanWorker implements RequestHandler<ScanInput, ScanOutput> {
    public ScanWorker(WorkerContext context) {
        super(context);
    }

    @Override
    public ScanOutput handleRequest(ScanInput event) {
        return process(event);
    }
}
