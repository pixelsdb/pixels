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

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.task.Task;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.coordinate.PlanCoordinator;
import io.pixelsdb.pixels.planner.coordinate.StageCoordinator;
import io.pixelsdb.pixels.planner.coordinate.StageDependency;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2022-07-05
 */
public abstract class AggregationOperator extends Operator
{
    /**
     * The inputs of the final aggregation workers that aggregate the
     * partial aggregation results produced by the child or the scan workers.
     */
    protected final List<AggregationInput> finalAggrInputs;
    /**
     * The scan inputs of the scan workers that produce the partial aggregation
     * results. It should be empty if child is not null.
     */
    protected final List<ScanInput> scanInputs;
    /**
     * The child operator that produce the partial aggregation results. It
     * should be null if scanInputs is not empty.
     */
    protected Operator child = null;
    /**
     * The outputs of the scan workers.
     */
    protected CompletableFuture<? extends Output>[] scanOutputs = null;
    /**
     * The outputs of the final aggregation workers.
     */
    protected CompletableFuture<? extends Output>[] finalAggrOutputs = null;

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
    public void initPlanCoordinator(PlanCoordinator planCoordinator, int parentStageId, boolean wideDependOnParent)
    {
        int aggrStageId = planCoordinator.assignStageId();
        StageDependency aggrStageDependency = new StageDependency(aggrStageId, parentStageId, wideDependOnParent);
        StageCoordinator aggrStageCoordinator = new StageCoordinator(aggrStageId, this.finalAggrInputs.size());
        planCoordinator.addStageCoordinator(aggrStageCoordinator, aggrStageDependency);
        if (this.scanInputs != null)
        {
            checkArgument(this.child == null,
                    "child operator should be null when base table scan exists");
            int scanStageId = planCoordinator.assignStageId();
            StageDependency scanStageDependency = new StageDependency(scanStageId, aggrStageId, true);
            List<Task> tasks = new ArrayList<>();
            int taskId = 0;
            for (ScanInput scanInput : this.scanInputs)
            {
                List<InputSplit> inputSplits = scanInput.getTableInfo().getInputSplits();
                for (InputSplit inputSplit : inputSplits)
                {
                    scanInput.getTableInfo().setInputSplits(ImmutableList.of(inputSplit));
                    tasks.add(new Task(taskId++, JSON.toJSONString(scanInput)));
                }
            }
            StageCoordinator scanStageCoordinator = new StageCoordinator(scanStageId, tasks);
            planCoordinator.addStageCoordinator(scanStageCoordinator, scanStageDependency);
        }
        else
        {
            requireNonNull(this.child, "child operator should not be null");
            this.child.initPlanCoordinator(planCoordinator, aggrStageId, true);
        }
    }

    @Override
    public OutputCollection collectOutputs() throws ExecutionException, InterruptedException
    {
        AggregationOutputCollection outputCollection = new AggregationOutputCollection();

        if (this.scanOutputs != null && this.scanOutputs.length > 0)
        {
            Output[] outputs = new Output[this.scanOutputs.length];
            for (int i = 0; i < this.scanOutputs.length; ++i)
            {
                outputs[i] = this.scanOutputs[i].get();
            }
            outputCollection.setScanOutputs(outputs);
        }
        if (this.child != null)
        {
            outputCollection.setChild(this.child.collectOutputs());
        }
        if (this.finalAggrOutputs != null && this.finalAggrOutputs.length > 0)
        {
            Output[] outputs = new Output[this.finalAggrOutputs.length];
            for (int i = 0; i < this.finalAggrOutputs.length; ++i)
            {
                outputs[i] = this.finalAggrOutputs[i].get();
            }
            outputCollection.setFinalAggrOutputs(outputs);
        }
        return outputCollection;
    }

    public static class AggregationOutputCollection implements OutputCollection
    {
        private Output[] scanOutputs = null;
        private OutputCollection child;
        private Output[] finalAggrOutputs = null;

        public AggregationOutputCollection() { }

        public AggregationOutputCollection(Output[] scanOutputs, OutputCollection child, Output[] preAggrOutputs)
        {
            this.scanOutputs = scanOutputs;
            this.child = child;
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

        public OutputCollection getChild()
        {
            return child;
        }

        public void setChild(OutputCollection child)
        {
            this.child = child;
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
            if (child != null)
            {
                totalGBMs += child.getTotalGBMs();
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
            if (child != null)
            {
                numReadRequests += child.getTotalNumReadRequests();
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
            if (this.child != null)
            {
                numWriteRequests += this.child.getTotalNumWriteRequests();
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
            if (this.child != null)
            {
                readBytes += this.child.getTotalReadBytes();
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
            if (this.child != null)
            {
                writeBytes += this.child.getTotalWriteBytes();
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
