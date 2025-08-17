/*
 * Copyright 2024 PixelsDB.
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
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.coordinate.PlanCoordinator;
import io.pixelsdb.pixels.planner.coordinate.StageCoordinator;
import io.pixelsdb.pixels.planner.coordinate.StageDependency;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.SortInput;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class SortedJoinOperator extends SingleStageJoinOperator
{
    protected final List<SortInput> smallSortedInputs;
    protected final List<SortInput> largeSortedInputs;
    protected CompletableFuture<? extends Output>[] smallSortedOutputs = null;
    protected CompletableFuture<? extends Output>[] largeSortedOutputs = null;

    public SortedJoinOperator(String name, List<SortInput> smallSortedInputs,
                              List<SortInput> largeSortedInputs,
                              List<JoinInput> joinInputs, JoinAlgorithm joinAlgo)
    {
        super(name, true, joinInputs, joinAlgo);
        if (joinAlgo != JoinAlgorithm.SORTED)
        {
            throw new UnsupportedOperationException("join algorithm '" + joinAlgo + "' is not supported");
        }
        if (smallSortedInputs == null || smallSortedInputs.isEmpty())
        {
            this.smallSortedInputs = ImmutableList.of();
        } else
        {
            this.smallSortedInputs = ImmutableList.copyOf(smallSortedInputs);
            for (SortInput SortedInput : this.smallSortedInputs)
            {
                SortedInput.setOperatorName(name);
            }
        }
        if (largeSortedInputs == null || largeSortedInputs.isEmpty())
        {
            this.largeSortedInputs = ImmutableList.of();
        } else
        {
            this.largeSortedInputs = ImmutableList.copyOf(largeSortedInputs);
            for (SortInput SortedInput : this.largeSortedInputs)
            {
                SortedInput.setOperatorName(name);
            }
        }
    }

    @Override
    public void setSmallChild(JoinOperator child)
    {
        if (child == null)
        {
            checkArgument(!this.smallSortedInputs.isEmpty(),
                    "smallSortedInputs must be non-empty if smallChild is set to null");
            this.smallChild = null;
        } else
        {
            checkArgument(this.smallSortedInputs.isEmpty(),
                    "smallSortedInputs must be empty if smallChild is set to non-null");
            this.smallChild = child;
        }
    }

    @Override
    public void setLargeChild(JoinOperator child)
    {
        if (child == null)
        {
            checkArgument(!this.largeSortedInputs.isEmpty(),
                    "largeSortedInputs must be non-empty if largeChild is set to null");
            this.largeChild = null;
        } else
        {
            checkArgument(this.largeSortedInputs.isEmpty(),
                    "largeSortedInputs must be empty if largeChild is set to non-null");
            this.largeChild = child;
        }
    }

    public List<SortInput> getSmallSortedInputs()
    {
        return smallSortedInputs;
    }

    public List<SortInput> getLargeSortedInputs()
    {
        return largeSortedInputs;
    }

    @Override
    public void initPlanCoordinator(PlanCoordinator planCoordinator, int parentStageId, boolean wideDependOnParent)
    {
        int joinStageId = planCoordinator.assignStageId();
        StageDependency joinStageDependency = new StageDependency(joinStageId, parentStageId, wideDependOnParent);
        StageCoordinator joinStageCoordinator = new StageCoordinator(joinStageId, this.joinInputs.size());
        planCoordinator.addStageCoordinator(joinStageCoordinator, joinStageDependency);
        if (this.joinAlgo == JoinAlgorithm.SORTED)
        {
            if (smallChild != null && largeChild != null)
            {
                // both children exist, we should execute both children and wait for the small child.
                checkArgument(smallSortedInputs.isEmpty(), "smallSortedInputs is not empty");
                checkArgument(largeSortedInputs.isEmpty(), "largeSortedInputs is not empty");
                this.smallChild.initPlanCoordinator(planCoordinator, joinStageId, true);
                this.largeChild.initPlanCoordinator(planCoordinator, joinStageId, true);

            } else if (smallChild != null)
            {
                // only small child exists, we should invoke the large table sorting and wait for the small child.
                checkArgument(smallSortedInputs.isEmpty(), "smallSortedInputs is not empty");
                checkArgument(!largeSortedInputs.isEmpty(), "largeSortedInputs is empty");
                this.smallChild.initPlanCoordinator(planCoordinator, joinStageId, true);

            } else if (largeChild != null)
            {
                // only large child exists, we should invoke and wait for the small table sorting.
                checkArgument(!smallSortedInputs.isEmpty(), "smallSortedInputs is empty");
                checkArgument(largeSortedInputs.isEmpty(), "largeSortedInputs is not empty");
                this.largeChild.initPlanCoordinator(planCoordinator, joinStageId, true);
            } else
            {
                checkArgument(!smallSortedInputs.isEmpty(), "smallSortedInputs is empty");
                checkArgument(!largeSortedInputs.isEmpty(), "largeSortedInputs is empty");
            }

            if (!smallSortedInputs.isEmpty())
            {
                int sortStageId = planCoordinator.assignStageId();
                StageDependency sortStageDependency = new StageDependency(sortStageId, joinStageId, true);
                List<Task> tasks = new ArrayList<>();
                int taskId = 0;
                for (SortInput SortedInput : this.smallSortedInputs)
                {
                    List<InputSplit> inputSplits = SortedInput.getTableInfo().getInputSplits();
                    for (InputSplit inputSplit : inputSplits)
                    {
                        SortedInput.getTableInfo().setInputSplits(ImmutableList.of(inputSplit));
                        tasks.add(new Task(taskId++, JSON.toJSONString(SortedInput)));
                    }
                }
                StageCoordinator sortStageCoordinator = new StageCoordinator(sortStageId, tasks);
                planCoordinator.addStageCoordinator(sortStageCoordinator, sortStageDependency);
            }

            if (!largeSortedInputs.isEmpty())
            {
                int sortStageId = planCoordinator.assignStageId();
                StageDependency sortStageDependency = new StageDependency(sortStageId, joinStageId, true);
                List<Task> tasks = new ArrayList<>();
                int taskId = 0;
                for (SortInput SortedInput : this.largeSortedInputs)
                {
                    List<InputSplit> inputSplits = SortedInput.getTableInfo().getInputSplits();
                    for (InputSplit inputSplit : inputSplits)
                    {
                        SortedInput.getTableInfo().setInputSplits(ImmutableList.of(inputSplit));
                        tasks.add(new Task(taskId++, JSON.toJSONString(SortedInput)));
                    }
                }
                StageCoordinator sortStageCoordinator = new StageCoordinator(sortStageId, tasks);
                planCoordinator.addStageCoordinator(sortStageCoordinator, sortStageDependency);
            }
        } else
        {
            throw new UnsupportedOperationException("join algorithm '" + joinAlgo + "' is unsupported");
        }
    }

    @Override
    public JoinOutputCollection collectOutputs() throws ExecutionException, InterruptedException
    {
        SortedJoinOutputCollection outputCollection = new SortedJoinOutputCollection();
        outputCollection.setJoinAlgo(joinAlgo);
        if (joinOutputs != null)
        {
            Output[] outputs = new Output[joinOutputs.length];
            for (int i = 0; i < joinOutputs.length; ++i)
            {
                outputs[i] = joinOutputs[i].get();
            }
            outputCollection.setJoinOutputs(outputs);
        }
        if (smallSortedOutputs != null)
        {
            Output[] outputs = new Output[smallSortedOutputs.length];
            for (int i = 0; i < smallSortedOutputs.length; ++i)
            {
                outputs[i] = smallSortedOutputs[i].get();
            }
            outputCollection.setSmallSortedOutputs(outputs);
        }
        if (largeSortedOutputs != null)
        {
            Output[] outputs = new Output[largeSortedOutputs.length];
            for (int i = 0; i < largeSortedOutputs.length; ++i)
            {
                outputs[i] = largeSortedOutputs[i].get();
            }
            outputCollection.setLargeSortedOutputs(outputs);
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

    public static class SortedJoinOutputCollection extends SingleStageJoinOutputCollection
    {
        protected Output[] smallSortedOutputs = null;
        protected Output[] largeSortedOutputs = null;

        public SortedJoinOutputCollection()
        {
        }

        public SortedJoinOutputCollection(JoinAlgorithm joinAlgo,
                                          OutputCollection smallChild,
                                          OutputCollection largeChild,
                                          Output[] joinOutputs,
                                          Output[] smallSortedOutputs,
                                          Output[] largeSortedOutputs)
        {
            super(joinAlgo, smallChild, largeChild, joinOutputs);
            this.smallSortedOutputs = smallSortedOutputs;
            this.largeSortedOutputs = largeSortedOutputs;
        }

        @Override
        public long getTotalGBMs()
        {
            long totalGBMs = super.getTotalGBMs();
            if (this.smallSortedOutputs != null)
            {
                for (Output output : smallSortedOutputs)
                {
                    totalGBMs += output.getGBMs();
                }
            }
            if (this.largeSortedOutputs != null)
            {
                for (Output output : largeSortedOutputs)
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
            if (this.smallSortedOutputs != null)
            {
                for (Output output : smallSortedOutputs)
                {
                    numReadRequests += output.getNumReadRequests();
                }
            }
            if (this.largeSortedOutputs != null)
            {
                for (Output output : largeSortedOutputs)
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
            if (this.smallSortedOutputs != null)
            {
                for (Output output : smallSortedOutputs)
                {
                    numWriteRequests += output.getNumWriteRequests();
                }
            }
            if (this.largeSortedOutputs != null)
            {
                for (Output output : largeSortedOutputs)
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
            if (this.smallSortedOutputs != null)
            {
                for (Output output : smallSortedOutputs)
                {
                    readBytes += output.getTotalReadBytes();
                }
            }
            if (this.largeSortedOutputs != null)
            {
                for (Output output : largeSortedOutputs)
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
            if (this.smallSortedOutputs != null)
            {
                for (Output output : smallSortedOutputs)
                {
                    writeBytes += output.getTotalWriteBytes();
                }
            }
            if (this.largeSortedOutputs != null)
            {
                for (Output output : largeSortedOutputs)
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

        public long getSmallSortedInputCostMs()
        {
            long inputCostMs = 0;
            if (this.smallSortedOutputs != null)
            {
                for (Output output : smallSortedOutputs)
                {
                    inputCostMs += output.getCumulativeInputCostMs();
                }
            }
            return inputCostMs;
        }

        public long getSmallSortComputeCostMs()
        {
            long computeCostMs = 0;
            if (this.smallSortedOutputs != null)
            {
                for (Output output : smallSortedOutputs)
                {
                    computeCostMs += output.getCumulativeComputeCostMs();
                }
            }
            return computeCostMs;
        }

        public long getSmallSortOutputCostMs()
        {
            long outputCostMs = 0;
            if (this.smallSortedOutputs != null)
            {
                for (Output output : smallSortedOutputs)
                {
                    outputCostMs += output.getCumulativeOutputCostMs();
                }
            }
            return outputCostMs;
        }

        public long getLargeSortedInputCostMs()
        {
            long inputCostMs = 0;
            if (this.largeSortedOutputs != null)
            {
                for (Output output : largeSortedOutputs)
                {
                    inputCostMs += output.getCumulativeInputCostMs();
                }
            }
            return inputCostMs;
        }

        public long getLargeSortComputeCostMs()
        {
            long computeCostMs = 0;
            if (this.largeSortedOutputs != null)
            {
                for (Output output : largeSortedOutputs)
                {
                    computeCostMs += output.getCumulativeComputeCostMs();
                }
            }
            return computeCostMs;
        }

        public long getLargeSortOutputCostMs()
        {
            long outputCostMs = 0;
            if (this.largeSortedOutputs != null)
            {
                for (Output output : largeSortedOutputs)
                {
                    outputCostMs += output.getCumulativeOutputCostMs();
                }
            }
            return outputCostMs;
        }

        public Output[] getSmallSortedOutputs()
        {
            return smallSortedOutputs;
        }

        public void setSmallSortedOutputs(Output[] smallSortedOutputs)
        {
            this.smallSortedOutputs = smallSortedOutputs;
        }

        public Output[] getLargeSortedOutputs()
        {
            return largeSortedOutputs;
        }

        public void setLargeSortedOutputs(Output[] largeSortedOutputs)
        {
            this.largeSortedOutputs = largeSortedOutputs;
        }
    }
}
