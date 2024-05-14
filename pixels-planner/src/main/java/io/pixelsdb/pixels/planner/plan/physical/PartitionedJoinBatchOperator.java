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
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.planner.plan.physical.OperatorExecutor.waitForCompletion;

/**
 * @author hank
 * @create 2023-09-19
 */
public class PartitionedJoinBatchOperator extends PartitionedJoinOperator
{
    private static final Logger logger = LogManager.getLogger(PartitionedJoinBatchOperator.class);

    public PartitionedJoinBatchOperator(String name, List<PartitionInput> smallPartitionInputs,
                                        List<PartitionInput> largePartitionInputs,
                                        List<JoinInput> joinInputs, JoinAlgorithm joinAlgo)
    {
        super(name, smallPartitionInputs, largePartitionInputs, joinInputs, joinAlgo);
    }

    /**
     * Execute this join operator.
     *
     * @return the completable future of the completable futures of the join outputs.
     */
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
                if (joinAlgo == JoinAlgorithm.PARTITIONED)
                {
                    joinOutputs[i] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.PARTITIONED_JOIN).invoke(joinInput);
                }
                else if (joinAlgo == JoinAlgorithm.PARTITIONED_CHAIN)
                {
                    joinOutputs[i] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.PARTITIONED_CHAIN_JOIN).invoke(joinInput);
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
                CompletableFuture<CompletableFuture<? extends Output>[]> largeChildFuture = null;
                if (smallChild != null && largeChild != null)
                {
                    // both children exist, we should execute both children and wait for the small child.
                    checkArgument(smallPartitionInputs.isEmpty(), "smallPartitionInputs is not empty");
                    checkArgument(largePartitionInputs.isEmpty(), "largePartitionInputs is not empty");
                    smallChildFuture = smallChild.execute();
                    largeChildFuture = largeChild.execute();
                    waitForCompletion(smallChildFuture.join());
                    waitForCompletion(largeChildFuture.join(), LargeSideCompletionRatio);
                    prevStagesFuture.complete(null);
                } else if (smallChild != null)
                {
                    // only small child exists, we should invoke the large table partitioning and wait for the small child.
                    checkArgument(smallPartitionInputs.isEmpty(), "smallPartitionInputs is not empty");
                    checkArgument(!largePartitionInputs.isEmpty(), "largePartitionInputs is empty");
                    smallChildFuture = smallChild.execute();
                    largePartitionOutputs = new CompletableFuture[largePartitionInputs.size()];
                    int i = 0;
                    for (PartitionInput partitionInput : largePartitionInputs)
                    {
                        largePartitionOutputs[i++] = InvokerFactory.Instance()
                                .getInvoker(WorkerType.PARTITION).invoke((partitionInput));
                    }

                    logger.debug("invoke large partition of " + this.getName());

                    waitForCompletion(smallChildFuture.join());
                    waitForCompletion(largePartitionOutputs, LargeSideCompletionRatio);
                    prevStagesFuture.complete(null);
                } else if (largeChild != null)
                {
                    // only large child exists, we should invoke and wait for the small table partitioning.
                    checkArgument(!smallPartitionInputs.isEmpty(), "smallPartitionInputs is empty");
                    checkArgument(largePartitionInputs.isEmpty(), "largePartitionInputs is not empty");
                    smallPartitionOutputs = new CompletableFuture[smallPartitionInputs.size()];
                    int i = 0;
                    for (PartitionInput partitionInput : smallPartitionInputs)
                    {
                        smallPartitionOutputs[i++] = InvokerFactory.Instance()
                                .getInvoker(WorkerType.PARTITION).invoke((partitionInput));
                    }

                    logger.debug("invoke small partition of " + this.getName());

                    largeChildFuture = largeChild.execute();
                    waitForCompletion(smallPartitionOutputs);
                    waitForCompletion(largeChildFuture.join(), LargeSideCompletionRatio);
                    prevStagesFuture.complete(null);
                } else
                {
                    // no children exist, partition both tables and wait for the small table partitioning.
                    checkArgument(!smallPartitionInputs.isEmpty(), "smallPartitionInputs is empty");
                    checkArgument(!largePartitionInputs.isEmpty(), "largePartitionInputs is empty");
                    smallPartitionOutputs = new CompletableFuture[smallPartitionInputs.size()];
                    int i = 0;
                    for (PartitionInput partitionInput : smallPartitionInputs)
                    {
                        smallPartitionOutputs[i++] = InvokerFactory.Instance()
                                .getInvoker(WorkerType.PARTITION).invoke((partitionInput));
                    }

                    logger.debug("invoke small partition of " + this.getName());

                    largePartitionOutputs = new CompletableFuture[largePartitionInputs.size()];
                    i = 0;
                    for (PartitionInput partitionInput : largePartitionInputs)
                    {
                        largePartitionOutputs[i++] = InvokerFactory.Instance()
                                .getInvoker(WorkerType.PARTITION).invoke((partitionInput));
                    }

                    logger.debug("invoke large partition of " + this.getName());

                    waitForCompletion(smallPartitionOutputs);
                    waitForCompletion(largePartitionOutputs, LargeSideCompletionRatio);
                    prevStagesFuture.complete(null);
                }
            }
            catch (InterruptedException e)
            {
                throw new CompletionException("interrupted when waiting for the completion of previous stages", e);
            }
        });

        return prevStagesFuture;
    }
}
