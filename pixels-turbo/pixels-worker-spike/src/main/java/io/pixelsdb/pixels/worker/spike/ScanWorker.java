package io.pixelsdb.pixels.worker.spike;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.BaseScanWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;

public class ScanWorker  extends BaseScanWorker implements WorkerInterface<ScanInput, ScanOutput>
{
    public ScanWorker(WorkerContext context)
    {
        super(context);
    }

    @Override
    public ScanOutput handleRequest(ScanInput event)
    {
        return process(event);
    }

    @Override
    public String getRequestId()
    {
        return this.context.getRequestId();
    }

    @Override
    public WorkerType getWorkerType()
    {
        return WorkerType.SCAN;
    }
}
