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
package io.pixelsdb.pixels.planner.plan.physical;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinInput;
import io.pixelsdb.pixels.common.turbo.Output;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * The executor of a single-stage join.
 *
 * @author hank
 * @create 2022-06-04
 */
public class SingleStageJoinOperator extends JoinOperator
{
    private static final Logger logger = LogManager.getLogger(SingleStageJoinOperator.class);
    protected final List<JoinInput> joinInputs;
    protected final JoinAlgorithm joinAlgo;
    protected JoinOperator smallChild = null;
    protected JoinOperator largeChild = null;
    protected CompletableFuture<?>[] joinOutputs = null;

    public SingleStageJoinOperator(String name, JoinInput joinInput, JoinAlgorithm joinAlgo)
    {
        super(name);
        // ImmutableList.of() add the reference of joinInput into the returned list
        joinInput.setOperatorName(name);
        this.joinInputs = ImmutableList.of(joinInput);
        this.joinAlgo = joinAlgo;
    }

    public SingleStageJoinOperator(String name, List<JoinInput> joinInputs, JoinAlgorithm joinAlgo)
    {
        super(name);
        this.joinInputs = ImmutableList.copyOf(joinInputs);
        for (JoinInput joinInput : this.joinInputs)
        {
            joinInput.setOperatorName(name);
        }
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
     * @return the completable future of the completable futures of the join outputs.
     */
    @Override
    public CompletableFuture<CompletableFuture<?>[]> execute()
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
                            .getInvoker(WorkerType.BROADCAST_JOIN).invoke(joinInput);
                }
                else if (joinAlgo == JoinAlgorithm.BROADCAST_CHAIN)
                {
                    joinOutputs[i] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.BROADCAST_CHAIN_JOIN).invoke(joinInput);
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
                CompletableFuture<CompletableFuture<?>[]> smallChildFuture = null;
                if (smallChild != null)
                {
                    smallChildFuture = smallChild.execute();
                }
                CompletableFuture<CompletableFuture<?>[]> largeChildFuture = null;
                if (largeChild != null)
                {
                    largeChildFuture = largeChild.execute();
                }
                if (smallChildFuture != null)
                {
                    CompletableFuture<?>[] smallChildOutputs = smallChildFuture.join();
                    waitForCompletion(smallChildOutputs);
                }
                if (largeChildFuture != null)
                {
                    CompletableFuture<?>[] largeChildOutputs = largeChildFuture.join();
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

        public SingleStageJoinOutputCollection(
                JoinAlgorithm joinAlgo, OutputCollection smallChild, OutputCollection largeChild, Output[] joinOutputs)
        {
            super(joinAlgo, smallChild, largeChild);
            this.joinOutputs = joinOutputs;
        }

        @Override
        public long getTotalGBMs()
        {
            long totalGBMs = super.getTotalGBMs();
            if (this.joinOutputs != null)
            {
                for (Output output : joinOutputs)
                {
                    totalGBMs += output.getGBMs();
                }
            }
            return totalGBMs;
        }

        @Override
        public int getTotalNumReadRequests()
        {
            int numReadRequests = super.getTotalNumReadRequests();
            if (this.joinOutputs != null)
            {
                for (Output output : joinOutputs)
                {
                    numReadRequests += output.getNumReadRequests();
                }
            }
            return numReadRequests;
        }

        @Override
        public int getTotalNumWriteRequests()
        {
            int numWriteRequests = super.getTotalNumWriteRequests();
            if (this.joinOutputs != null)
            {
                for (Output output : joinOutputs)
                {
                    numWriteRequests += output.getNumWriteRequests();
                }
            }
            return numWriteRequests;
        }

        @Override
        public long getTotalReadBytes()
        {
            long readBytes = super.getTotalReadBytes();
            if (this.joinOutputs != null)
            {
                for (Output output : joinOutputs)
                {
                    readBytes += output.getTotalReadBytes();
                }
            }
            return readBytes;
        }

        @Override
        public long getTotalWriteBytes()
        {
            long writeBytes = super.getTotalWriteBytes();
            if (this.joinOutputs != null)
            {
                for (Output output : joinOutputs)
                {
                    writeBytes += output.getTotalWriteBytes();
                }
            }
            return writeBytes;
        }

        @Override
        public long getLayerInputCostMs()
        {
            long inputCostMs = 0;
            if (this.joinOutputs != null)
            {
                for (Output output : joinOutputs)
                {
                    inputCostMs += output.getCumulativeInputCostMs();
                }
            }
            return inputCostMs;
        }

        @Override
        public long getLayerComputeCostMs()
        {
            long computeCostMs = 0;
            if (this.joinOutputs != null)
            {
                for (Output output : joinOutputs)
                {
                    computeCostMs += output.getCumulativeComputeCostMs();
                }
            }
            return computeCostMs;
        }

        @Override
        public long getLayerOutputCostMs()
        {
            long outputCostMs = 0;
            if (this.joinOutputs != null)
            {
                for (Output output : joinOutputs)
                {
                    outputCostMs += output.getCumulativeOutputCostMs();
                }
            }
            return outputCostMs;
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
