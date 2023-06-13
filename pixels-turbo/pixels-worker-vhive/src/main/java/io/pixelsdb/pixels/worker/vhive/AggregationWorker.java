/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.worker.common.BaseAggregationWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;

public class AggregationWorker extends BaseAggregationWorker implements RequestHandler<AggregationInput, AggregationOutput>
{
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
