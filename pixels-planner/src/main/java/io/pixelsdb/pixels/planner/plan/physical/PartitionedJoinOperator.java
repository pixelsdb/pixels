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
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.common.turbo.Output;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The executor of a partitioned join.
 *
 * @author hank
 * @create 2022-06-04
 */
public class PartitionedJoinOperator extends SingleStageJoinOperator
{
    private static final Logger logger = LogManager.getLogger(PartitionedJoinOperator.class);
    protected final List<PartitionInput> smallPartitionInputs;
    protected final List<PartitionInput> largePartitionInputs;
    protected CompletableFuture<?>[] smallPartitionOutputs = null;
    protected CompletableFuture<?>[] largePartitionOutputs = null;

    public PartitionedJoinOperator(String name, List<PartitionInput> smallPartitionInputs,
                                   List<PartitionInput> largePartitionInputs,
                                   List<JoinInput> joinInputs, JoinAlgorithm joinAlgo)
    {
        super(name, joinInputs, joinAlgo);
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
                if (joinAlgo == JoinAlgorithm.PARTITIONED)
                {
                    joinOutputs[i] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.PARTITIONED_JOIN).invoke(joinInputs.get(i));
                }
                else if (joinAlgo == JoinAlgorithm.PARTITIONED_CHAIN)
                {
                    joinOutputs[i] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.PARTITIONED_JOIN).invoke(joinInputs.get(i));
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
                CompletableFuture<CompletableFuture<?>[]> largeChildFuture = null;
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

    @Override
    public JoinOutputCollection collectOutputs() throws ExecutionException, InterruptedException
    {
        PartitionedJoinOutputCollection outputCollection = new PartitionedJoinOutputCollection();
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
        if (smallPartitionOutputs != null)
        {
            Output[] outputs = new Output[smallPartitionOutputs.length];
            for (int i = 0; i < smallPartitionOutputs.length; ++i)
            {
                outputs[i] = (Output) smallPartitionOutputs[i].get();
            }
            outputCollection.setSmallPartitionOutputs(outputs);
        }
        if (largePartitionOutputs != null)
        {
            Output[] outputs = new Output[largePartitionOutputs.length];
            for (int i = 0; i < largePartitionOutputs.length; ++i)
            {
                outputs[i] = (Output) largePartitionOutputs[i].get();
            }
            outputCollection.setLargePartitionOutputs(outputs);
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

    public static class PartitionedJoinOutputCollection extends SingleStageJoinOutputCollection
    {
        protected Output[] smallPartitionOutputs = null;
        protected Output[] largePartitionOutputs = null;

        public PartitionedJoinOutputCollection() { }

        public PartitionedJoinOutputCollection(JoinAlgorithm joinAlgo,
                                               OutputCollection smallChild,
                                               OutputCollection largeChild,
                                               Output[] joinOutputs,
                                               Output[] smallPartitionOutputs,
                                               Output[] largePartitionOutputs)
        {
            super(joinAlgo, smallChild, largeChild, joinOutputs);
            this.smallPartitionOutputs = smallPartitionOutputs;
            this.largePartitionOutputs = largePartitionOutputs;
        }

        @Override
        public long getTotalGBMs()
        {
            long totalGBMs = super.getTotalGBMs();
            if (this.smallPartitionOutputs != null)
            {
                for (Output output : smallPartitionOutputs)
                {
                    totalGBMs += output.getGBMs();
                }
            }
            if (this.largePartitionOutputs != null)
            {
                for (Output output : largePartitionOutputs)
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
            if (this.smallPartitionOutputs != null)
            {
                for (Output output : smallPartitionOutputs)
                {
                    numReadRequests += output.getNumReadRequests();
                }
            }
            if (this.largePartitionOutputs != null)
            {
                for (Output output : largePartitionOutputs)
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
            if (this.smallPartitionOutputs != null)
            {
                for (Output output : smallPartitionOutputs)
                {
                    numWriteRequests += output.getNumWriteRequests();
                }
            }
            if (this.largePartitionOutputs != null)
            {
                for (Output output : largePartitionOutputs)
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
            if (this.smallPartitionOutputs != null)
            {
                for (Output output : smallPartitionOutputs)
                {
                    readBytes += output.getTotalReadBytes();
                }
            }
            if (this.largePartitionOutputs != null)
            {
                for (Output output : largePartitionOutputs)
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
            if (this.smallPartitionOutputs != null)
            {
                for (Output output : smallPartitionOutputs)
                {
                    writeBytes += output.getTotalWriteBytes();
                }
            }
            if (this.largePartitionOutputs != null)
            {
                for (Output output : largePartitionOutputs)
                {
                    writeBytes += output.getTotalWriteBytes();
                }
            }
            return writeBytes;
        }

        @Override
        public long getLayerInputCostMs()
        {
            return super.getLayerInputCostMs();
        }

        @Override
        public long getLayerComputeCostMs()
        {
            return super.getLayerComputeCostMs();
        }

        @Override
        public long getLayerOutputCostMs()
        {
            return super.getLayerOutputCostMs();
        }

        public long getSmallPartitionInputCostMs()
        {
            long inputCostMs = 0;
            if (this.smallPartitionOutputs != null)
            {
                for (Output output : smallPartitionOutputs)
                {
                    inputCostMs += output.getCumulativeInputCostMs();
                }
            }
            return inputCostMs;
        }

        public long getSmallPartitionComputeCostMs()
        {
            long computeCostMs = 0;
            if (this.smallPartitionOutputs != null)
            {
                for (Output output : smallPartitionOutputs)
                {
                    computeCostMs += output.getCumulativeComputeCostMs();
                }
            }
            return computeCostMs;
        }

        public long getSmallPartitionOutputCostMs()
        {
            long outputCostMs = 0;
            if (this.smallPartitionOutputs != null)
            {
                for (Output output : smallPartitionOutputs)
                {
                    outputCostMs += output.getCumulativeOutputCostMs();
                }
            }
            return outputCostMs;
        }

        public long getLargePartitionInputCostMs()
        {
            long inputCostMs = 0;
            if (this.largePartitionOutputs != null)
            {
                for (Output output : largePartitionOutputs)
                {
                    inputCostMs += output.getCumulativeInputCostMs();
                }
            }
            return inputCostMs;
        }

        public long getLargePartitionComputeCostMs()
        {
            long computeCostMs = 0;
            if (this.largePartitionOutputs != null)
            {
                for (Output output : largePartitionOutputs)
                {
                    computeCostMs += output.getCumulativeComputeCostMs();
                }
            }
            return computeCostMs;
        }

        public long getLargePartitionOutputCostMs()
        {
            long outputCostMs = 0;
            if (this.largePartitionOutputs != null)
            {
                for (Output output : largePartitionOutputs)
                {
                    outputCostMs += output.getCumulativeOutputCostMs();
                }
            }
            return outputCostMs;
        }

        public Output[] getSmallPartitionOutputs()
        {
            return smallPartitionOutputs;
        }

        public void setSmallPartitionOutputs(Output[] smallPartitionOutputs)
        {
            this.smallPartitionOutputs = smallPartitionOutputs;
        }

        public Output[] getLargePartitionOutputs()
        {
            return largePartitionOutputs;
        }

        public void setLargePartitionOutputs(Output[] largePartitionOutputs)
        {
            this.largePartitionOutputs = largePartitionOutputs;
        }
    }
}
