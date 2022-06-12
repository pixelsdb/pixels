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
        if (joinAlgo != JoinAlgorithm.PARTITIONED)
        {
            throw new UnsupportedOperationException("join algorithm '" + joinAlgo + "' is not supported");
        }
        if (smallPartitionInputs == null)
        {
            this.smallPartitionInputs = ImmutableList.of();
        }
        else
        {
            this.smallPartitionInputs = ImmutableList.copyOf(smallPartitionInputs);
        }
        if (largePartitionInputs == null)
        {
            this.largePartitionInputs = ImmutableList.of();
        }
        else
        {
            this.largePartitionInputs = ImmutableList.copyOf(largePartitionInputs);
        }
        checkArgument(!this.smallPartitionInputs.isEmpty() || !this.largePartitionInputs.isEmpty(),
                "both smallPartitionInputs and largePartitionInputs are empty");
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
        executePrev();
        CompletableFuture<?>[] joinOutputs = new CompletableFuture[joinInputs.size()];
        for (int i = 0; i < joinInputs.size(); ++i)
        {
            joinOutputs[i] = PartitionedJoinInvoker.invoke((PartitionedJoinInput) joinInputs.get(i));
        }
        return joinOutputs;
    }

    @Override
    public CompletableFuture<?>[] executePrev()
    {
        if (child != null)
        {
            if (smallChild)
            {
                // child is on the small side, we should invoke the large table partitioning and wait for the child.
                checkArgument(smallPartitionInputs.isEmpty(), "smallPartitionInputs is not empty");
                checkArgument(!largePartitionInputs.isEmpty(), "largePartitionInputs is empty");
                CompletableFuture<?>[] childOutputs = child.execute();
                for (PartitionInput partitionInput : largePartitionInputs)
                {
                    PartitionInvoker.invoke((partitionInput));
                }
                waitForCompletion(childOutputs);
                return childOutputs;
            }
            else
            {
                // child is on the large side, we should invoke and wait for the small table partitioning.
                checkArgument(!smallPartitionInputs.isEmpty(), "smallPartitionInputs is empty");
                checkArgument(largePartitionInputs.isEmpty(), "largePartitionInputs is not empty");
                CompletableFuture<?>[] smallPartitionOutputs =
                        new CompletableFuture[smallPartitionInputs.size()];
                int i = 0;
                for (PartitionInput partitionInput : smallPartitionInputs)
                {
                    smallPartitionOutputs[i++] = PartitionInvoker.invoke((partitionInput));
                }
                child.execute();
                waitForCompletion(smallPartitionOutputs);
                return smallPartitionOutputs;
            }
        }
        else
        {
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
            waitForCompletion(smallPartitionOutputs);
            return smallPartitionOutputs;
        }
    }
}
