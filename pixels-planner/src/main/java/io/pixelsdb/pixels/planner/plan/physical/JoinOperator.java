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

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.coordinate.StageCoordinator;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinInput;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author hank
 * @create 2022-06-05
 */
public abstract class JoinOperator extends Operator
{
    protected static final double LargeSideCompletionRatio;

    static
    {
        String ratio = ConfigFactory.Instance().getProperty("join.large.side.completion.ratio");
        LargeSideCompletionRatio = Double.parseDouble(ratio);
    }

    /**
     * Issue #482:
     * Whether this join operator is completely constructed and can be executed.
     * During the construction of broadcast chain join, intermediate join operators might be incomplete.
     */
    private final boolean complete;

    public JoinOperator(String name, boolean complete)
    {
        super(name);
        this.complete = complete;
    }

    /**
     * @return true if this join operator is completely constructed and can be executed.
     */
    public boolean isComplete()
    {
        return complete;
    }

    public abstract List<JoinInput> getJoinInputs();

    public abstract JoinAlgorithm getJoinAlgo();

    public abstract void setSmallChild(JoinOperator child);

    public abstract void setLargeChild(JoinOperator child);

    public abstract JoinOperator getSmallChild();

    public abstract JoinOperator getLargeChild();

    protected StageCoordinator createJoinStageCoordinator(StageCoordinator parentStageCoordinator, int joinStageId, int workerNum)
    {
        StageCoordinator joinStageCoordinator;
        if (parentStageCoordinator != null)
        {
            if (parentStageCoordinator.leftChildWorkerIsEmpty())
            {
                joinStageCoordinator = new StageCoordinator(joinStageId, workerNum);
                parentStageCoordinator.setLeftChildWorkerNum(workerNum);
            } else
            {
                joinStageCoordinator = new StageCoordinator(joinStageId, workerNum,
                        parentStageCoordinator.getLeftChildWorkerNum());
                parentStageCoordinator.setRightChildWorkerNum(workerNum);
            }
            joinStageCoordinator.setDownStreamWorkerNum(parentStageCoordinator.getFixedWorkerNum());
        } else
        {
            joinStageCoordinator = new StageCoordinator(joinStageId, workerNum);
        }
        return joinStageCoordinator;
    }

    /**
     * This method collects the outputs of the join operator. It may block until the join
     * completes, therefore it should not be called in the query execution thread. Otherwise,
     * it will block the query execution.
     * @return the out
     */
    public abstract JoinOutputCollection collectOutputs() throws ExecutionException, InterruptedException;

    /**
     * This class is used to collect the outputs of a join operator.
     * It can be serialized to a json string or deserialized to an object from json string
     * by fastjson or other feasible json libraries.
     */
    public static abstract class JoinOutputCollection implements OutputCollection
    {
        protected JoinAlgorithm joinAlgo;
        protected OutputCollection smallChild;
        protected OutputCollection largeChild;

        public JoinOutputCollection() { }

        public JoinOutputCollection(JoinAlgorithm joinAlgo, OutputCollection smallChild, OutputCollection largeChild)
        {
            this.joinAlgo = joinAlgo;
            this.smallChild = smallChild;
            this.largeChild = largeChild;
        }

        public JoinAlgorithm getJoinAlgo()
        {
            return joinAlgo;
        }

        public void setJoinAlgo(JoinAlgorithm joinAlgo)
        {
            this.joinAlgo = joinAlgo;
        }

        public OutputCollection getSmallChild()
        {
            return smallChild;
        }

        public void setSmallChild(OutputCollection smallChild)
        {
            this.smallChild = smallChild;
        }

        public OutputCollection getLargeChild()
        {
            return largeChild;
        }

        public void setLargeChild(OutputCollection largeChild)
        {
            this.largeChild = largeChild;
        }

        @Override
        public long getTotalGBMs()
        {
            long totalGBMs= 0;
            if (this.smallChild != null)
            {
                totalGBMs += smallChild.getTotalGBMs();
            }
            if (this.largeChild != null)
            {
                totalGBMs += largeChild.getTotalGBMs();
            }
            return totalGBMs;
        }

        @Override
        public int getTotalNumReadRequests()
        {
            int numReadRequests = 0;
            if (this.smallChild != null)
            {
                numReadRequests += smallChild.getTotalNumReadRequests();
            }
            if (this.largeChild != null)
            {
                numReadRequests += largeChild.getTotalNumReadRequests();
            }
            return numReadRequests;
        }

        @Override
        public int getTotalNumWriteRequests()
        {
            int numWriteRequests = 0;
            if (this.smallChild != null)
            {
                numWriteRequests += smallChild.getTotalNumWriteRequests();
            }
            if (this.largeChild != null)
            {
                numWriteRequests += largeChild.getTotalNumWriteRequests();
            }
            return numWriteRequests;
        }

        @Override
        public long getTotalReadBytes()
        {
            long readBytes = 0;
            if (this.smallChild != null)
            {
                readBytes += smallChild.getTotalReadBytes();
            }
            if (this.largeChild != null)
            {
                readBytes += largeChild.getTotalReadBytes();
            }
            return readBytes;
        }

        @Override
        public long getTotalWriteBytes()
        {
            long writeBytes = 0;
            if (this.smallChild != null)
            {
                writeBytes += smallChild.getTotalWriteBytes();
            }
            if (this.largeChild != null)
            {
                writeBytes += largeChild.getTotalWriteBytes();
            }
            return writeBytes;
        }
    }
}
