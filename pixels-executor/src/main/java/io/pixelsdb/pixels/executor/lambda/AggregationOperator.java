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
import io.pixelsdb.pixels.executor.lambda.input.AggregationInput;
import io.pixelsdb.pixels.executor.lambda.input.ScanInput;
import io.pixelsdb.pixels.executor.lambda.output.Output;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.executor.lambda.SingleStageJoinOperator.waitForCompletion;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @date 05/07/2022
 */
public class AggregationOperator implements Operator
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

    public AggregationOperator(AggregationInput finalAggrInput,
                               List<AggregationInput> preAggrInputs,
                               List<ScanInput> scanInputs)
    {
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
    public CompletableFuture<?>[] execute()
    {
        waitForCompletion(executePrev());
        requireNonNull(this.finalAggrInput, "finalAggrInput is null");
        this.finalAggrOutput = new CompletableFuture[1];
        this.finalAggrOutput[0] = InvokerFactory.Instance()
                .getInvoker(WorkerType.AGGREGATION).invoke(this.finalAggrInput);
        return this.finalAggrOutput;
    }

    @Override
    public CompletableFuture<?>[] executePrev()
    {
        if (this.child != null)
        {
            checkArgument(this.scanInputs.isEmpty(), "scanInputs is not empty");
            this.scanOutputs = new CompletableFuture[0];
            this.child.execute();
        }
        else
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
        if (this.preAggrInputs.isEmpty())
        {
            this.preAggrOutputs = new CompletableFuture[0];
        }
        else
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
            return this.preAggrOutputs;
        }
        return this.scanOutputs;
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
    }
}
