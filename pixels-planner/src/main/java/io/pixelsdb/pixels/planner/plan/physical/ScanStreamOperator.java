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
package io.pixelsdb.pixels.planner.plan.physical;

import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author hank
 * @create 2024-04-28
 */
public class ScanStreamOperator extends ScanOperator
{
    private static final CompletableFuture<Void> Completed = CompletableFuture.completedFuture(null);
    public ScanStreamOperator(String name, List<ScanInput> scanInputs)
    {
        super(name, scanInputs);
    }

    @Override
    public CompletableFuture<CompletableFuture<? extends Output>[]> execute()
    {
        return executePrev().handle((result, exception) ->
        {
            // there is no previous stage for scan operator, hence executePrev() never throws exceptions
            // stream scan will invoke workers less than scanInputs
            this.scanOutputs = new CompletableFuture[this.scanInputs.size()];
            int i = 0;
            for (ScanInput scanInput : this.scanInputs)
            {
                this.scanOutputs[i++] = InvokerFactory.Instance()
                        .getInvoker(WorkerType.SCAN_STREAMING).invoke(scanInput);
            }
            return this.scanOutputs;
        });
    }

    @Override
    public CompletableFuture<Void> executePrev()
    {
        return Completed;
    }
}
