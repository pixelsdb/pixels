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

import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.lambda.input.JoinInput;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author hank
 * @date 05/06/2022
 */
public abstract class JoinOperator extends Operator
{
    public abstract List<JoinInput> getJoinInputs();

    public abstract JoinAlgorithm getJoinAlgo();

    public abstract void setSmallChild(JoinOperator child);

    public abstract void setLargeChild(JoinOperator child);

    public abstract JoinOperator getSmallChild();

    public abstract JoinOperator getLargeChild();

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
    public static class JoinOutputCollection implements OutputCollection
    {
        protected JoinAlgorithm joinAlgo;
        protected OutputCollection smallChild;
        protected OutputCollection largeChild;

        public JoinOutputCollection() { }

        public JoinOutputCollection(JoinAlgorithm joinAlgo,
                                OutputCollection smallChild,
                                OutputCollection largeChild)
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
        public long getCumulativeDurationMs()
        {
            long duration = 0;
            if (this.smallChild != null)
            {
                duration += smallChild.getCumulativeDurationMs();
            }
            if (this.largeChild != null)
            {
                duration += largeChild.getCumulativeDurationMs();
            }
            return duration;
        }
    }
}
