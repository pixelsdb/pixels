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
import io.pixelsdb.pixels.executor.lambda.output.JoinOutput;
import io.pixelsdb.pixels.executor.lambda.output.PartitionOutput;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The executor of a partitioned join.
 *
 * @author hank
 * @date 04/06/2022
 */
public class PartitionedJoinOperator extends SingleStageJoinOperator
{
    private final List<PartitionInput> leftPartitionInputs;
    private final List<PartitionInput> rightPartitionInputs;

    public PartitionedJoinOperator(List<PartitionInput> leftPartitionInputs,
                                   List<PartitionInput> rightPartitionInputs,
                                   List<JoinInput> joinInputs, JoinAlgorithm joinAlgo)
    {
        super(joinInputs, joinAlgo);
        if (joinAlgo != JoinAlgorithm.PARTITIONED)
        {
            throw new UnsupportedOperationException("join algorithm '" + joinAlgo + "' is not supported");
        }
        if (leftPartitionInputs == null)
        {
            this.leftPartitionInputs = ImmutableList.of();
        }
        else
        {
            this.leftPartitionInputs = ImmutableList.copyOf(leftPartitionInputs);
        }
        ;
        this.rightPartitionInputs = ImmutableList.copyOf(requireNonNull(rightPartitionInputs,
                "rightPartitionInputs is null"));
        checkArgument(!this.rightPartitionInputs.isEmpty(), "rightPartitionInputs is empty");
    }

    public List<PartitionInput> getLeftPartitionInputs()
    {
        return leftPartitionInputs;
    }

    public List<PartitionInput> getRightPartitionInputs()
    {
        return rightPartitionInputs;
    }

    /**
     * Execute this join operator.
     *
     * @return the join outputs.
     */
    @Override
    public CompletableFuture<JoinOutput>[] execute()
    {
        if (child != null)
        {
            CompletableFuture<JoinOutput>[] childOutputs = child.execute();
            checkArgument(leftPartitionInputs.isEmpty(), "leftPartitionInputs is not empty");
            for (PartitionInput partitionInput : rightPartitionInputs)
            {
                PartitionInvoker.invoke((partitionInput));
            }
            waitForCompletion(childOutputs);
        }
        else
        {
            CompletableFuture<PartitionOutput>[] leftPartitionOutputs =
                    new CompletableFuture[leftPartitionInputs.size()];
            int i = 0;
            for (PartitionInput partitionInput : leftPartitionInputs)
            {
                leftPartitionOutputs[i++] = PartitionInvoker.invoke((partitionInput));
            }
            for (PartitionInput partitionInput : leftPartitionInputs)
            {
                PartitionInvoker.invoke((partitionInput));
            }
            waitForCompletion(leftPartitionOutputs);
        }
        CompletableFuture<JoinOutput>[] joinOutputs = new CompletableFuture[joinInputs.size()];
        for (int i = 0; i < joinInputs.size(); ++i)
        {
            joinOutputs[i] = PartitionedJoinInvoker.invoke((PartitionedJoinInput) joinInputs.get(i));
        }
        return joinOutputs;
    }
}
