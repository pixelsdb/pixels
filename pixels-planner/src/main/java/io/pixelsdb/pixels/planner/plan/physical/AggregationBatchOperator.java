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
package io.pixelsdb.pixels.planner.plan.physical;

import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.planner.plan.physical.OperatorExecutor.waitForCompletion;

/**
 * @author hank
 * @create 2023-09-19
 */
public class AggregationBatchOperator extends AggregationOperator
{
    public AggregationBatchOperator(String name, List<AggregationInput> finalAggrInputs, List<ScanInput> scanInputs)
    {
        super(name, finalAggrInputs, scanInputs);
    }

    @Override
    public CompletableFuture<CompletableFuture<? extends Output>[]> execute()
    {
        return executePrev().handle((result, exception) ->
        {
            if (exception != null)
            {
                throw new CompletionException("failed to complete the previous stages", exception);
            }

            try
            {
                this.finalAggrOutputs = new CompletableFuture[this.finalAggrInputs.size()];
                int i = 0;
                for (AggregationInput aggrInput : this.finalAggrInputs)
                {
                    this.finalAggrOutputs[i++] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.AGGREGATION).invoke(aggrInput);
                }
                waitForCompletion(this.finalAggrOutputs);
            } catch (InterruptedException e)
            {
                throw new CompletionException("interrupted when waiting for the completion of this operator", e);
            }

            return this.finalAggrOutputs;
        });
    }

    @Override
    public CompletableFuture<Void> executePrev()
    {
        CompletableFuture<Void> prevStagesFuture = new CompletableFuture<>();
        operatorService.execute(() ->
        {
            try
            {
                CompletableFuture<CompletableFuture<? extends Output>[]> childFuture = null;
                if (this.child != null)
                {
                    checkArgument(this.scanInputs.isEmpty(), "scanInputs is not empty");
                    this.scanOutputs = new CompletableFuture[0];
                    childFuture = this.child.execute();
                } else
                {
                    checkArgument(!this.scanInputs.isEmpty(), "scanInputs is empty");
                    this.scanOutputs = new CompletableFuture[this.scanInputs.size()];
                    int i = 0;
                    for (ScanInput scanInput : this.scanInputs)
                    {
                        this.scanOutputs[i++] = InvokerFactory.Instance()
                                .getInvoker(WorkerType.SCAN).invoke(scanInput);
                    }
                }

                if (childFuture != null)
                {
                    waitForCompletion(childFuture.join());
                }
                if (this.scanOutputs.length > 0)
                {
                    waitForCompletion(this.scanOutputs);
                }

                prevStagesFuture.complete(null);
            }
            catch (InterruptedException e)
            {
                throw new CompletionException("interrupted when waiting for the completion of previous stages", e);
            }
        });

        return prevStagesFuture;
    }
}
