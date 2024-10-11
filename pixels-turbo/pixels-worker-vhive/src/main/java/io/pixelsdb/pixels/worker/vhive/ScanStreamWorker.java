/*
 * Copyright 2024 PixelsDB.
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
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.BaseScanStreamWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;

/**
 * Process a scan input under HTTP Streaming mode.
 * Implemented c.f. {@link io.pixelsdb.pixels.worker.vhive.utils.RequestHandler}.
 *
 * @author huasiy
 * @create 2024-10-10
 */
public class ScanStreamWorker extends BaseScanStreamWorker implements RequestHandler<ScanInput, ScanOutput>
{
    public ScanStreamWorker(WorkerContext context)
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
        return WorkerType.SCAN_STREAM;
    }
}
