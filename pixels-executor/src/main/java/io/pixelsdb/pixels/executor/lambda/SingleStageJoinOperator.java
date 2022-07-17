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
import io.pixelsdb.pixels.executor.lambda.output.Output;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * The executor of a single-stage join.
 *
 * @author hank
 * @date 04/06/2022
 */
public class SingleStageJoinOperator extends JoinOperator
{
    protected final List<JoinInput> joinInputs;
    protected final JoinAlgorithm joinAlgo;
    protected JoinOperator smallChild = null;
    protected JoinOperator largeChild = null;
    protected CompletableFuture<?>[] joinOutputs = null;

    public SingleStageJoinOperator(JoinInput joinInput, JoinAlgorithm joinAlgo)
    {
        this.joinInputs = ImmutableList.of(joinInput);
        this.joinAlgo = joinAlgo;
    }

    public SingleStageJoinOperator(List<JoinInput> joinInputs, JoinAlgorithm joinAlgo)
    {
        this.joinInputs = ImmutableList.copyOf(joinInputs);
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
    public void setSmallChild(JoinOperator child)
    {
        this.smallChild = child;
    }

    @Override
    public void setLargeChild(JoinOperator child)
    {
        this.largeChild = child;
    }

    @Override
    public JoinOperator getSmallChild()
    {
        return this.smallChild;
    }

    @Override
    public JoinOperator getLargeChild()
    {
        return this.largeChild;
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
        joinOutputs = new CompletableFuture[joinInputs.size()];
        for (int i = 0; i < joinInputs.size(); ++i)
        {
            if (joinAlgo == JoinAlgorithm.BROADCAST)
            {
                joinOutputs[i] = InvokerFactory.Instance()
                        .getInvoker(WorkerType.BROADCAST_JOIN).invoke(joinInputs.get(i));
            }
            else if (joinAlgo == JoinAlgorithm.BROADCAST_CHAIN)
            {
                joinOutputs[i] = InvokerFactory.Instance()
                        .getInvoker(WorkerType.BROADCAST_CHAIN_JOIN).invoke(joinInputs.get(i));
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
        CompletableFuture<?>[] smallChildOutputs = null;
        if (smallChild != null)
        {
            smallChildOutputs = smallChild.execute();
        }
        if (largeChild != null)
        {
            largeChild.execute();
        }
        if (smallChildOutputs != null)
        {
            return smallChildOutputs;
        }
        return new CompletableFuture[0];
    }

    @Override
    public JoinOutputCollection collectOutputs() throws ExecutionException, InterruptedException
    {
        SingleStageJoinOutputCollection outputCollection = new SingleStageJoinOutputCollection();
        outputCollection.setJoinAlgo(joinAlgo);
        if (joinOutputs != null)
        {
            Output[] outputs = new Output[joinOutputs.length];
            for (int i = 0; i < joinOutputs.length; ++i)
            {
                outputs[i] = (Output) joinOutputs[i].get();
            }
            outputCollection.setJoinOutputs(outputs);
        }
        if (smallChild != null)
        {
            outputCollection.setSmallChild(smallChild.collectOutputs());
        }
        if (largeChild != null)
        {
            outputCollection.setLargeChild(largeChild.collectOutputs());
        }
        return outputCollection;
    }

    public static class SingleStageJoinOutputCollection extends JoinOutputCollection
    {
        protected Output[] joinOutputs = null;

        public SingleStageJoinOutputCollection() { }

        public SingleStageJoinOutputCollection(JoinAlgorithm joinAlgo,
                                               OutputCollection smallChild,
                                               OutputCollection largeChild,
                                               Output[] joinOutputs)
        {
            super(joinAlgo, smallChild, largeChild);
            this.joinOutputs = joinOutputs;
        }

        public Output[] getJoinOutputs()
        {
            return joinOutputs;
        }

        public void setJoinOutputs(Output[] joinOutputs)
        {
            this.joinOutputs = joinOutputs;
        }
    }
}
