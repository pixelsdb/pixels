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

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.lambda.input.JoinInput;
import io.pixelsdb.pixels.executor.lambda.input.PartitionInput;
import io.pixelsdb.pixels.executor.lambda.input.PartitionedChainJoinInput;
import io.pixelsdb.pixels.executor.lambda.input.PartitionedJoinInput;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The executor of a partitioned join.
 *
 * @author hank
 * @date 04/06/2022
 */
public class PartitionedJoinOperator extends SingleStageJoinOperator
{
    private final List<PartitionInput> smallPartitionInputs;
    private final List<PartitionInput> largePartitionInputs;

    public PartitionedJoinOperator(List<PartitionInput> smallPartitionInputs,
                                   List<PartitionInput> largePartitionInputs,
                                   List<JoinInput> joinInputs, JoinAlgorithm joinAlgo)
    {
        super(joinInputs, joinAlgo);
        if (joinAlgo != JoinAlgorithm.PARTITIONED && joinAlgo != JoinAlgorithm.PARTITIONED_CHAIN)
        {
            throw new UnsupportedOperationException("join algorithm '" + joinAlgo + "' is not supported");
        }
        if (smallPartitionInputs == null || smallPartitionInputs.isEmpty())
        {
            this.smallPartitionInputs = ImmutableList.of();
        }
        else
        {
            this.smallPartitionInputs = ImmutableList.copyOf(smallPartitionInputs);
        }
        if (largePartitionInputs == null || largePartitionInputs.isEmpty())
        {
            this.largePartitionInputs = ImmutableList.of();
        }
        else
        {
            this.largePartitionInputs = ImmutableList.copyOf(largePartitionInputs);
        }
    }

    @Override
    public void setSmallChild(JoinOperator child)
    {
        if (child == null)
        {
            checkArgument(!this.smallPartitionInputs.isEmpty(),
                    "smallPartitionInputs must be non-empty if smallChild is set to null");
            this.smallChild = null;
        }
        else
        {
            checkArgument(this.smallPartitionInputs.isEmpty(),
                    "smallPartitionInputs must be empty if smallChild is set to non-null");
            this.smallChild = child;
        }
    }

    @Override
    public void setLargeChild(JoinOperator child)
    {
        if (child == null)
        {
            checkArgument(!this.largePartitionInputs.isEmpty(),
                    "largePartitionInputs must be non-empty if largeChild is set to null");
            this.largeChild = null;
        }
        else
        {
            checkArgument(this.largePartitionInputs.isEmpty(),
                    "largePartitionInputs must be empty if largeChild is set to non-null");
            this.largeChild = child;
        }
    }

    public List<PartitionInput> getSmallPartitionInputs()
    {
        return smallPartitionInputs;
    }

    public List<PartitionInput> getLargePartitionInputs()
    {
        return largePartitionInputs;
    }

    /**
     * Execute this join operator.
     *
     * @return the join outputs.
     */
    @Override
    public CompletableFuture<?>[] execute()
    {
        waitForCompletion(executePrev());
        CompletableFuture<?>[] joinOutputs = new CompletableFuture[joinInputs.size()];
        for (int i = 0; i < joinInputs.size(); ++i)
        {
            if (joinAlgo == JoinAlgorithm.PARTITIONED)
            {
                joinOutputs[i] = PartitionedJoinInvoker.invoke((PartitionedJoinInput) joinInputs.get(i));
            }
            else if (joinAlgo == JoinAlgorithm.PARTITIONED_CHAIN)
            {
                joinOutputs[i] = PartitionedChainJoinInvoker.invoke((PartitionedChainJoinInput) joinInputs.get(i));
            }
            else
            {
                throw new UnsupportedOperationException("join algorithm '" + joinAlgo + "' is unsupported");
            }
        }
        return joinOutputs;
    }

    @Override
    public CompletableFuture<?>[] executePrev()
    {
        if (smallChild != null && largeChild != null)
        {
            // both children exist, we should execute both children and wait for the small child.
            checkArgument(smallPartitionInputs.isEmpty(), "smallPartitionInputs is not empty");
            checkArgument(largePartitionInputs.isEmpty(), "largePartitionInputs is not empty");
            CompletableFuture<?>[] childOutputs = smallChild.execute();
            largeChild.execute();
            return childOutputs;
        }
        else if (smallChild != null)
        {
            // only small child exists, we should invoke the large table partitioning and wait for the small child.
            checkArgument(smallPartitionInputs.isEmpty(), "smallPartitionInputs is not empty");
            checkArgument(!largePartitionInputs.isEmpty(), "largePartitionInputs is empty");
            CompletableFuture<?>[] childOutputs = smallChild.execute();
            for (PartitionInput partitionInput : largePartitionInputs)
            {
                PartitionInvoker.invoke((partitionInput));
            }
            return childOutputs;
        }
        else if (largeChild != null)
        {
            // only large child exists, we should invoke and wait for the small table partitioning.
            checkArgument(!smallPartitionInputs.isEmpty(), "smallPartitionInputs is empty");
            checkArgument(largePartitionInputs.isEmpty(), "largePartitionInputs is not empty");
            CompletableFuture<?>[] smallPartitionOutputs =
                    new CompletableFuture[smallPartitionInputs.size()];
            int i = 0;
            for (PartitionInput partitionInput : smallPartitionInputs)
            {
                smallPartitionOutputs[i++] = PartitionInvoker.invoke((partitionInput));
            }
            largeChild.execute();
            return smallPartitionOutputs;
        }
        else
        {
            // no children exist, partition both tables and wait for the small table partitioning.
            CompletableFuture<?>[] smallPartitionOutputs =
                    new CompletableFuture[smallPartitionInputs.size()];
            int i = 0;
            for (PartitionInput partitionInput : smallPartitionInputs)
            {
                smallPartitionOutputs[i++] = PartitionInvoker.invoke((partitionInput));
            }
            for (PartitionInput partitionInput : largePartitionInputs)
            {
                PartitionInvoker.invoke((partitionInput));
            }
            return smallPartitionOutputs;
        }
    }
}
