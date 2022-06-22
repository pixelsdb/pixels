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
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.lambda.input.BroadcastChainJoinInput;
import io.pixelsdb.pixels.executor.lambda.input.BroadcastJoinInput;
import io.pixelsdb.pixels.executor.lambda.input.JoinInput;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The executor of a single-stage join.
 *
 * @author hank
 * @date 04/06/2022
 */
public class SingleStageJoinOperator implements JoinOperator
{
    protected static final double StageCompletionRatio;
    protected final List<JoinInput> joinInputs;
    protected final JoinAlgorithm joinAlgo;
    protected JoinOperator child = null;
    protected boolean smallChild = false;

    static
    {
        StageCompletionRatio = Double.parseDouble(
                ConfigFactory.Instance().getProperty("join.stage.completion.ratio"));
    }

    public SingleStageJoinOperator(JoinInput joinInput, JoinAlgorithm joinAlgo)
    {
        this.joinInputs = ImmutableList.of(joinInput);
        this.joinAlgo = joinAlgo;
    }

    public SingleStageJoinOperator(List<JoinInput> joinInputs, JoinAlgorithm joinAlgo)
    {
        this.joinInputs = joinInputs;
        this.joinAlgo = joinAlgo;
    }

    @Override
    public List<JoinInput> getJoinInputs()
    {
        return joinInputs;
    }

    @Override
    public JoinAlgorithm getJoinAlgo()
    {
        return joinAlgo;
    }

    @Override
    public void setChild(JoinOperator child, boolean smallChild)
    {
        this.child = child;
        this.smallChild = smallChild;
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
            if (joinAlgo == JoinAlgorithm.BROADCAST)
            {
                joinOutputs[i] = BroadcastJoinInvoker.invoke((BroadcastJoinInput) joinInputs.get(i));
            }
            else if (joinAlgo == JoinAlgorithm.BROADCAST_CHAIN)
            {
                joinOutputs[i] = BroadcastChainJoinInvoker.invoke((BroadcastChainJoinInput) joinInputs.get(i));
            }
            else
            {
                throw new UnsupportedOperationException("join algorithm '" + joinAlgo + "' is unknown");
            }
        }
        return joinOutputs;
    }

    @Override
    public CompletableFuture<?>[] executePrev()
    {
        if (child != null)
        {
            CompletableFuture<?>[] childOutputs = child.execute();
            if (smallChild)
            {
                // if the child is on the small side, we should wait for its completion.
                waitForCompletion(childOutputs);
            }
            return childOutputs;
        }
        return new CompletableFuture[0];
    }

    protected static void waitForCompletion(CompletableFuture<?>[] childOutputs)
    {
        while (true)
        {
            double completed = 0;
            for (CompletableFuture<?> childOutput : childOutputs)
            {
                if (childOutput.isDone())
                {
                    checkArgument(!childOutput.isCompletedExceptionally(),
                            "child join worker is completed exceptionally");
                    checkArgument(!childOutput.isCancelled(),
                            "child join worker is cancelled");
                    completed++;
                }
            }

            if (completed / childOutputs.length >= StageCompletionRatio)
            {
                break;
            }
        }
    }
}
