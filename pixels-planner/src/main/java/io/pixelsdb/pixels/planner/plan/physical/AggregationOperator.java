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
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.common.turbo.Output;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2022-07-05
 */
public class AggregationOperator extends Operator
{
    /**
     * The inputs of the final aggregation workers that aggregate the
     * partial aggregation results produced by the child or the scan workers.
     */
    private final List<AggregationInput> finalAggrInputs;
    /**
     * The scan inputs of the scan workers that produce the partial aggregation
     * results. It should be empty if child is not null.
     */
    private final List<ScanInput> scanInputs;
    /**
     * The child operator that produce the partial aggregation results. It
     * should be null if scanInputs is not empty.
     */
    private Operator child = null;
    /**
     * The outputs of the scan workers.
     */
    private CompletableFuture<?>[] scanOutputs = null;
    /**
     * The outputs of the final aggregation workers.
     */
    private CompletableFuture<?>[] finalAggrOutputs = null;

    public AggregationOperator(String name, List<AggregationInput> finalAggrInputs, List<ScanInput> scanInputs)
    {
        super(name);
        requireNonNull(finalAggrInputs, "finalAggrInputs is null");
        checkArgument(!finalAggrInputs.isEmpty(), "finalAggrInputs is empty");
        this.finalAggrInputs = ImmutableList.copyOf(finalAggrInputs);
        for (AggregationInput aggrInput : this.finalAggrInputs)
        {
            aggrInput.setOperatorName(name);
        }

        if (scanInputs == null || scanInputs.isEmpty())
        {
            this.scanInputs = ImmutableList.of();
        }
        else
        {
            this.scanInputs = ImmutableList.copyOf(scanInputs);
            for (ScanInput scanInput : this.scanInputs)
            {
                scanInput.setOperatorName(name);
            }
        }
    }

    public List<AggregationInput> getFinalAggrInputs()
    {
        return finalAggrInputs;
    }

    public List<ScanInput> getScanInputs()
    {
        return scanInputs;
    }

    public void setChild(Operator child)
    {
        if (child == null)
        {
            checkArgument(!this.scanInputs.isEmpty(),
                    "scanInputs must be non-empty if child is set to null");
            this.child = null;
        }
        else
        {
            checkArgument(this.scanInputs.isEmpty(),
                    "scanInputs must be empty if child is set to non-null");
            this.child = child;
        }
    }

    @Override
    public CompletableFuture<CompletableFuture<?>[]> execute()
    {
        return executePrev().handle((result, exception) ->
        {
            if (exception != null)
            {
                throw new CompletionException("failed to complete the previous stages", exception);
            }

            try
            {
                this.finalAggrOutputs = new CompletableFuture[this.finalAggrInputs.size()];
                int i = 0;
                for (AggregationInput preAggrInput : this.finalAggrInputs)
                {
                    this.finalAggrOutputs[i++] = InvokerFactory.Instance()
                            .getInvoker(WorkerType.AGGREGATION).invoke(preAggrInput);
                }
                waitForCompletion(this.finalAggrOutputs);
            } catch (InterruptedException e)
            {
                throw new CompletionException("interrupted when waiting for the completion of this operator", e);
            }

            return this.finalAggrOutputs;
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
                CompletableFuture<CompletableFuture<?>[]> childFuture = null;
                if (this.child != null)
                {
                    checkArgument(this.scanInputs.isEmpty(), "scanInputs is not empty");
                    this.scanOutputs = new CompletableFuture[0];
                    childFuture = this.child.execute();
                } else
                {
                    checkArgument(!this.scanInputs.isEmpty(), "scanInputs is empty");
                    this.scanOutputs = new CompletableFuture[this.scanInputs.size()];
                    int i = 0;
                    for (ScanInput scanInput : this.scanInputs)
                    {
                        this.scanOutputs[i++] = InvokerFactory.Instance()
                                .getInvoker(WorkerType.SCAN).invoke(scanInput);
                    }
                }

                if (childFuture != null)
                {
                    waitForCompletion(childFuture.join());
                }
                if (this.scanOutputs.length > 0)
                {
                    waitForCompletion(this.scanOutputs);
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
    public OutputCollection collectOutputs() throws ExecutionException, InterruptedException
    {
        AggregationOutputCollection outputCollection = new AggregationOutputCollection();

        if (this.scanOutputs.length > 0)
        {
            Output[] outputs = new Output[this.scanOutputs.length];
            for (int i = 0; i < this.scanOutputs.length; ++i)
            {
                outputs[i] = (Output) this.scanOutputs[i].get();
            }
            outputCollection.setScanOutputs(outputs);
        }
        if (this.finalAggrOutputs != null && this.finalAggrOutputs.length > 0)
        {
            Output[] outputs = new Output[this.finalAggrOutputs.length];
            for (int i = 0; i < this.finalAggrOutputs.length; ++i)
            {
                outputs[i] = (Output) this.finalAggrOutputs[i].get();
            }
            outputCollection.setFinalAggrOutputs(outputs);
        }
        return outputCollection;
    }

    public static class AggregationOutputCollection implements OutputCollection
    {
        private Output[] scanOutputs = null;
        private Output[] finalAggrOutputs = null;

        public AggregationOutputCollection() { }

        public AggregationOutputCollection(Output[] scanOutputs, Output[] preAggrOutputs)
        {
            this.scanOutputs = scanOutputs;
            this.finalAggrOutputs = preAggrOutputs;
        }

        public Output[] getScanOutputs()
        {
            return scanOutputs;
        }

        public void setScanOutputs(Output[] scanOutputs)
        {
            this.scanOutputs = scanOutputs;
        }

        public Output[] getFinalAggrOutputs()
        {
            return finalAggrOutputs;
        }

        public void setFinalAggrOutputs(Output[] preAggrOutputs)
        {
            this.finalAggrOutputs = preAggrOutputs;
        }

        @Override
        public long getTotalGBMs()
        {
            long totalGBMs = 0;
            if (this.scanOutputs != null)
            {
                for (Output output : scanOutputs)
                {
                    totalGBMs += output.getGBMs();
                }
            }
            if (this.finalAggrOutputs != null)
            {
                for (Output output : finalAggrOutputs)
                {
                    totalGBMs += output.getGBMs();
                }
            }
            return totalGBMs;
        }

        @Override
        public int getTotalNumReadRequests()
        {
            int numReadRequests = 0;
            if (this.scanOutputs != null)
            {
                for (Output output : scanOutputs)
                {
                    numReadRequests += output.getNumReadRequests();
                }
            }
            if (this.finalAggrOutputs != null)
            {
                for (Output output : finalAggrOutputs)
                {
                    numReadRequests += output.getNumReadRequests();
                }
            }
            return numReadRequests;
        }

        @Override
        public int getTotalNumWriteRequests()
        {
            int numWriteRequests = 0;
            if (this.scanOutputs != null)
            {
                for (Output output : scanOutputs)
                {
                    numWriteRequests += output.getNumWriteRequests();
                }
            }
            if (this.finalAggrOutputs != null)
            {
                for (Output output : finalAggrOutputs)
                {
                    numWriteRequests += output.getNumWriteRequests();
                }
            }
            return numWriteRequests;
        }

        @Override
        public long getTotalReadBytes()
        {
            long readBytes = 0;
            if (this.scanOutputs != null)
            {
                for (Output output : scanOutputs)
                {
                    readBytes += output.getTotalReadBytes();
                }
            }
            if (this.finalAggrOutputs != null)
            {
                for (Output output : finalAggrOutputs)
                {
                    readBytes += output.getTotalReadBytes();
                }
            }
            return readBytes;
        }

        @Override
        public long getTotalWriteBytes()
        {
            long writeBytes = 0;
            if (this.scanOutputs != null)
            {
                for (Output output : scanOutputs)
                {
                    writeBytes += output.getTotalWriteBytes();
                }
            }
            if (this.finalAggrOutputs != null)
            {
                for (Output output : finalAggrOutputs)
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
            if (this.finalAggrOutputs != null)
            {
                for (Output output : finalAggrOutputs)
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
            if (this.finalAggrOutputs != null)
            {
                for (Output output : finalAggrOutputs)
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
            if (this.finalAggrOutputs != null)
            {
                for (Output output : finalAggrOutputs)
                {
                    outputCostMs += output.getCumulativeOutputCostMs();
                }
            }
            return outputCostMs;
        }

        public long getScanInputCostMs()
        {
            long inputCostMs = 0;
            if (this.scanOutputs != null)
            {
                for (Output output : scanOutputs)
                {
                    inputCostMs += output.getCumulativeInputCostMs();
                }
            }
            return inputCostMs;
        }

        public long getScanComputeCostMs()
        {
            long computeCostMs = 0;
            if (this.scanOutputs != null)
            {
                for (Output output : scanOutputs)
                {
                    computeCostMs += output.getCumulativeComputeCostMs();
                }
            }
            return computeCostMs;
        }

        public long getScanOutputCostMs()
        {
            long outputCostMs = 0;
            if (this.scanOutputs != null)
            {
                for (Output output : scanOutputs)
                {
                    outputCostMs += output.getCumulativeOutputCostMs();
                }
            }
            return outputCostMs;
        }
    }
}
