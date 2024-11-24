package io.pixelsdb.pixels.worker.spike;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.worker.common.BaseAggregationWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;

public class AggregationWorker extends BaseAggregationWorker implements WorkerInterface<AggregationInput, AggregationOutput>{
    public AggregationWorker(WorkerContext context)
    {
        super(context);
    }

    @Override
    public AggregationOutput handleRequest(AggregationInput input)
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
        return WorkerType.AGGREGATION;
    }
}
