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
package io.pixelsdb.pixels.executor.lambda.operator;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.executor.lambda.InvokerFactory;
import io.pixelsdb.pixels.executor.lambda.WorkerType;
import io.pixelsdb.pixels.executor.lambda.input.AggregationInput;
import io.pixelsdb.pixels.executor.lambda.input.ScanInput;
import io.pixelsdb.pixels.executor.lambda.output.Output;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 05/07/2022
 */
public class AggregationOperator extends Operator
{
    /**
     * The input of the final aggregation worker that produce the
     * final aggregation result.
     */
    private final AggregationInput finalAggrInput;
    /**
     * The inputs of the aggregation workers that pre-aggregate the
     * partial aggregation results produced by the child or the scan workers.
     */
    private final List<AggregationInput> preAggrInputs;
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
     * The outputs of the pre-aggregation workers.
     */
    private CompletableFuture<?>[] preAggrOutputs = null;
    /**
     * The output of the final aggregation worker.
     * There should be only one element in this array.
     */
    private CompletableFuture<?>[] finalAggrOutput = null;

    public AggregationOperator(String name, AggregationInput finalAggrInput,
                               List<AggregationInput> preAggrInputs,
                               List<ScanInput> scanInputs)
    {
        super(name);
        this.finalAggrInput = requireNonNull(finalAggrInput, "aggregateInput is null");
        if (preAggrInputs == null || preAggrInputs.isEmpty())
        {
            this.preAggrInputs = ImmutableList.of();
        }
        else
        {
            this.preAggrInputs = ImmutableList.copyOf(preAggrInputs);
        }
        if (scanInputs == null || scanInputs.isEmpty())
        {
            this.scanInputs = ImmutableList.of();
        }
        else
        {
            this.scanInputs = ImmutableList.copyOf(scanInputs);
        }
    }

    public AggregationInput getFinalAggrInput()
    {
        return finalAggrInput;
    }

    public List<AggregationInput> getPreAggrInputs()
    {
        return preAggrInputs;
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

            requireNonNull(this.finalAggrInput, "finalAggrInput is null");
            this.finalAggrOutput = new CompletableFuture[1];
            this.finalAggrOutput[0] = InvokerFactory.Instance()
                    .getInvoker(WorkerType.AGGREGATION).invoke(this.finalAggrInput);
            return this.finalAggrOutput;
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

                if (this.preAggrInputs.isEmpty())
                {
                    this.preAggrOutputs = new CompletableFuture[0];
                } else
                {
                    this.preAggrOutputs = new CompletableFuture[this.preAggrInputs.size()];
                    int i = 0;
                    for (AggregationInput preAggrInput : this.preAggrInputs)
                    {
                        this.preAggrOutputs[i++] = InvokerFactory.Instance()
                                .getInvoker(WorkerType.AGGREGATION).invoke(preAggrInput);
                    }
                }
                if (this.preAggrOutputs.length > 0)
                {
                    waitForCompletion(this.preAggrOutputs);
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
        if (this.finalAggrOutput != null)
        {
            outputCollection.setFinalAggrOutput((Output) this.finalAggrOutput[0].get());
        }
        if (this.scanOutputs.length > 0)
        {
            Output[] outputs = new Output[this.scanOutputs.length];
            for (int i = 0; i < this.scanOutputs.length; ++i)
            {
                outputs[i] = (Output) this.scanOutputs[i].get();
            }
            outputCollection.setScanOutputs(outputs);
        }
        if (this.preAggrOutputs.length > 0)
        {
            Output[] outputs = new Output[this.preAggrOutputs.length];
            for (int i = 0; i < this.preAggrOutputs.length; ++i)
            {
                outputs[i] = (Output) this.preAggrOutputs[i].get();
            }
            outputCollection.setPreAggrOutputs(outputs);
        }
        return outputCollection;
    }

    public static class AggregationOutputCollection implements OutputCollection
    {
        private Output[] scanOutputs = null;
        private Output[] preAggrOutputs = null;
        private Output finalAggrOutput = null;

        public AggregationOutputCollection() { }

        public AggregationOutputCollection(Output[] scanOutputs, Output[] preAggrOutputs, Output finalAggrOutput)
        {
            this.scanOutputs = scanOutputs;
            this.preAggrOutputs = preAggrOutputs;
            this.finalAggrOutput = finalAggrOutput;
        }

        public Output[] getScanOutputs()
        {
            return scanOutputs;
        }

        public void setScanOutputs(Output[] scanOutputs)
        {
            this.scanOutputs = scanOutputs;
        }

        public Output[] getPreAggrOutputs()
        {
            return preAggrOutputs;
        }

        public void setPreAggrOutputs(Output[] preAggrOutputs)
        {
            this.preAggrOutputs = preAggrOutputs;
        }

        public Output getFinalAggrOutput()
        {
            return finalAggrOutput;
        }

        public void setFinalAggrOutput(Output finalAggrOutput)
        {
            this.finalAggrOutput = finalAggrOutput;
        }

        @Override
        public long getCumulativeDurationMs()
        {
            long duration = 0;
            if (this.scanOutputs != null)
            {
                for (Output output : scanOutputs)
                {
                    duration += output.getDurationMs();
                }
            }
            if (this.preAggrOutputs != null)
            {
                for (Output output : preAggrOutputs)
                {
                    duration += output.getDurationMs();
                }
            }
            if (this.finalAggrOutput != null)
            {
                duration += finalAggrOutput.getDurationMs();
            }
            return duration;
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
            if (this.preAggrOutputs != null)
            {
                for (Output output : preAggrOutputs)
                {
                    numReadRequests += output.getNumReadRequests();
                }
            }
            if (this.finalAggrOutput != null)
            {
                numReadRequests += finalAggrOutput.getNumReadRequests();
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
            if (this.preAggrOutputs != null)
            {
                for (Output output : preAggrOutputs)
                {
                    numWriteRequests += output.getNumWriteRequests();
                }
            }
            if (this.finalAggrOutput != null)
            {
                numWriteRequests += finalAggrOutput.getNumWriteRequests();
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
            if (this.preAggrOutputs != null)
            {
                for (Output output : preAggrOutputs)
                {
                    readBytes += output.getTotalReadBytes();
                }
            }
            if (this.finalAggrOutput != null)
            {
                readBytes += finalAggrOutput.getTotalReadBytes();
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
            if (this.preAggrOutputs != null)
            {
                for (Output output : preAggrOutputs)
                {
                    writeBytes += output.getTotalWriteBytes();
                }
            }
            if (this.finalAggrOutput != null)
            {
                writeBytes += finalAggrOutput.getTotalWriteBytes();
            }
            return writeBytes;
        }

        @Override
        public long getLayerInputCostMs()
        {
            if (this.finalAggrOutput != null)
            {
                return finalAggrOutput.getCumulativeInputCostMs();
            }
            return 0;
        }

        @Override
        public long getLayerComputeCostMs()
        {
            if (this.finalAggrOutput != null)
            {
                return finalAggrOutput.getCumulativeComputeCostMs();
            }
            return 0;
        }

        @Override
        public long getLayerOutputCostMs()
        {
            if (this.finalAggrOutput != null)
            {
                return finalAggrOutput.getCumulativeOutputCostMs();
            }
            return 0;
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

        public long getPreAggrInputCostMs()
        {
            long inputCostMs = 0;
            if (this.preAggrOutputs != null)
            {
                for (Output output : preAggrOutputs)
                {
                    inputCostMs += output.getCumulativeInputCostMs();
                }
            }
            return inputCostMs;
        }

        public long getPreAggrComputeCostMs()
        {
            long computeCostMs = 0;
            if (this.preAggrOutputs != null)
            {
                for (Output output : preAggrOutputs)
                {
                    computeCostMs += output.getCumulativeComputeCostMs();
                }
            }
            return computeCostMs;
        }

        public long getPreAggrOutputCostMs()
        {
            long outputCostMs = 0;
            if (this.preAggrOutputs != null)
            {
                for (Output output : preAggrOutputs)
                {
                    outputCostMs += output.getCumulativeOutputCostMs();
                }
            }
            return outputCostMs;
        }
    }
}
