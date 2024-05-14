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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.pixelsdb.pixels.planner.plan.physical.OperatorExecutor.waitForCompletion;

/**
 * @author hank
 * @create 2024-04-28
 */
public class ScanBatchOperator extends ScanOperator
{
    private static final Logger logger = LogManager.getLogger(ScanBatchOperator.class);

    private static final CompletableFuture<Void> Completed = CompletableFuture.completedFuture(null);

    public ScanBatchOperator(String name, List<ScanInput> scanInputs)
    {
        super(name, scanInputs);
    }

    @Override
    public CompletableFuture<CompletableFuture<? extends Output>[]> execute()
    {
        return executePrev().handle((result, exception) ->
        {
            // there is no previous stage for scan operator, hence executePrev() never throws exceptions
            try
            {
                this.scanOutputs = new CompletableFuture[this.scanInputs.size()];
                int i = 0;
                for (ScanInput scanInput : this.scanInputs)
                {
                    this.scanOutputs[i++] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.SCAN).invoke(scanInput);
                }
                waitForCompletion(this.scanOutputs);
            } catch (InterruptedException e)
            {
                throw new CompletionException("interrupted when waiting for the completion of this operator", e);
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
