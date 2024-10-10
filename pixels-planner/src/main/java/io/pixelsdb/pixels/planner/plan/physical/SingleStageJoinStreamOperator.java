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
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinInput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.pixelsdb.pixels.planner.plan.physical.OperatorExecutor.waitForCompletion;

/**
 * @author hank
 * @create 2023-09-19
 */
public class SingleStageJoinStreamOperator extends SingleStageJoinOperator
{
    private static final Logger logger = LogManager.getLogger(SingleStageJoinStreamOperator.class);

    public SingleStageJoinStreamOperator(String name, boolean complete,
                                         JoinInput joinInput, JoinAlgorithm joinAlgo)
    {
        super(name, complete, joinInput, joinAlgo);
    }

    public SingleStageJoinStreamOperator(String name, boolean complete,
                                         List<JoinInput> joinInputs, JoinAlgorithm joinAlgo)
    {
        super(name, complete, joinInputs, joinAlgo);
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
            joinOutputs = new CompletableFuture[joinInputs.size()];
            for (int i = 0; i < joinInputs.size(); ++i)
            {
                JoinInput joinInput = joinInputs.get(i);
                if (joinAlgo == JoinAlgorithm.BROADCAST)
                {
                    joinOutputs[i] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.BROADCAST_JOIN_STREAMING).invoke(joinInput);
                }
                else if (joinAlgo == JoinAlgorithm.BROADCAST_CHAIN)
                {
//                    joinOutputs[i] = InvokerFactory.Instance()
//                            .getInvoker(WorkerType.BROADCAST_CHAIN_JOIN).invoke(joinInput);
                    throw new UnsupportedOperationException("join algorithm '" + joinAlgo + "' is unsupported");
                }
                else
                {
                    throw new UnsupportedOperationException("join algorithm '" + joinAlgo + "' is unsupported");
                }
            }

            logger.debug("invoke " + this.getName());
            return joinOutputs;
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
                CompletableFuture<CompletableFuture<? extends Output>[]> smallChildFuture = null;
                if (smallChild != null)
                {
                    throw new InterruptedException();
                }
                CompletableFuture<CompletableFuture<? extends Output>[]> largeChildFuture = null;
                if (largeChild != null)
                {
                    throw new InterruptedException();
                }
                if (smallChildFuture != null)
                {
                    CompletableFuture<? extends Output>[] smallChildOutputs = smallChildFuture.join();
                    waitForCompletion(smallChildOutputs);
                }
                if (largeChildFuture != null)
                {
                    CompletableFuture<? extends Output>[] largeChildOutputs = largeChildFuture.join();
                    waitForCompletion(largeChildOutputs, LargeSideCompletionRatio);
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
