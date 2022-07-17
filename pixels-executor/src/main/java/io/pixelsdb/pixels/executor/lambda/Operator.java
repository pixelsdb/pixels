/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.executor.lambda;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 05/07/2022
 */
public abstract class Operator
{
    protected static final double StageCompletionRatio;

    static
    {
        StageCompletionRatio = Double.parseDouble(
                ConfigFactory.Instance().getProperty("executor.stage.completion.ratio"));
    }

    /**
     * Execute this operator recursively.
     *
     * @return the completable futures of the execution outputs.
     */
    public abstract CompletableFuture<?>[] execute();

    /**
     * Execute the previous stages (if any) before the last stage, recursively.
     * And return the completable futures of the outputs of the previous states that
     * we should wait for completion.
     * @return empty array if the previous stages do not exist or do not need to be wait for
     */
    public abstract CompletableFuture<?>[] executePrev();

    /**
     * This method collects the outputs of the operator. It may block until the join
     * completes, therefore it should not be called in the query execution thread. Otherwise,
     * it will block the query execution.
     * @return the out
     */
    public abstract OutputCollection collectOutputs() throws ExecutionException, InterruptedException;

    public interface OutputCollection { }

    public static void waitForCompletion(CompletableFuture<?>[] stageOutputs)
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

            if (completed / stageOutputs.length >= StageCompletionRatio)
            {
                break;
            }
        }
    }
}
