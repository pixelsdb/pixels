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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.planner.plan.physical.Operator.StageCompletionRatio;
import static java.util.Objects.requireNonNull;

/**
 * Some methods are moved from {@link Operator} to here.
 *
 * @author hank
 * @create 2023-09-16
 */
public interface OperatorExecutor
{
    /**
     * Execute this operator recursively.
     *
     * @return the completable future that completes when this operator is complete, and
     * provides the computable futures of the outputs of this operator
     */
    CompletableFuture<CompletableFuture<?>[]> execute();

    /**
     * Execute the previous stages (if any) before the last stage, recursively.
     * And return the completable future that completes when the previous states are complete.
     *
     * @return empty array if the previous stages do not exist or do not need to be waited for
     */
    CompletableFuture<Void> executePrev();

    /**
     * This method collects the outputs of the operator. It may block until the join
     * completes, therefore it should not be called in the query execution thread. Otherwise,
     * it will block the query execution.
     * @return the outputs of the workers in this operator
     */
    OutputCollection collectOutputs() throws ExecutionException, InterruptedException;

    interface OutputCollection
    {
        long getTotalGBMs();

        int getTotalNumReadRequests();

        int getTotalNumWriteRequests();

        long getTotalReadBytes();

        long getTotalWriteBytes();

        long getLayerInputCostMs();

        long getLayerComputeCostMs();

        long getLayerOutputCostMs();
    }

    static void waitForCompletion(CompletableFuture<?>[] stageOutputs) throws InterruptedException
    {
        waitForCompletion(stageOutputs, StageCompletionRatio);
    }

    static void waitForCompletion(CompletableFuture<?>[] stageOutputs, double completionRatio)
            throws InterruptedException
    {
        requireNonNull(stageOutputs, "stageOutputs is null");

        if (stageOutputs.length == 0)
        {
            return;
        }

        while (true)
        {
            double completed = 0;
            for (CompletableFuture<?> childOutput : stageOutputs)
            {
                if (childOutput.isDone())
                {
                    checkArgument(!childOutput.isCompletedExceptionally(),
                            "worker in the stage is completed exceptionally");
                    checkArgument(!childOutput.isCancelled(),
                            "worker in the stage is cancelled");
                    completed++;
                }
            }

            if (completed / stageOutputs.length >= completionRatio)
            {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }
}
