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

import io.pixelsdb.pixels.executor.lambda.ScanInput.OutputInfo;

import java.util.List;

/**
 * @author hank
 * @date 07/05/2022
 */
public class PartitionedJoinInput
{
    /**
     * The unique id of the query.
     */
    private int queryId;

    /**
     * The partitioned files of the left (small) table.
     */
    private List<PartitionOutput> leftPartitioned;

    /**
     * The partitioned files of the right (big) table.
     */
    private List<PartitionInput> rightPartitioned;

    /**
     * The information of tasks for the join worker.
     */
    private JoinInfo joinInfo;

    /**
     * The output information of the join worker.
     */
    private OutputInfo output;

    /**
     * Default constructor for Jackson.
     */
    public PartitionedJoinInput() { }

    public PartitionedJoinInput(int queryId, List<PartitionOutput> leftPartitioned,
                                List<PartitionInput> rightPartitioned, JoinInfo joinInfo,
                                OutputInfo output)
    {
        this.queryId = queryId;
        this.leftPartitioned = leftPartitioned;
        this.rightPartitioned = rightPartitioned;
        this.joinInfo = joinInfo;
        this.output = output;
    }

    public int getQueryId()
    {
        return queryId;
    }

    public void setQueryId(int queryId)
    {
        this.queryId = queryId;
    }

    public List<PartitionOutput> getLeftPartitioned()
    {
        return leftPartitioned;
    }

    public void setLeftPartitioned(List<PartitionOutput> leftPartitioned)
    {
        this.leftPartitioned = leftPartitioned;
    }

    public List<PartitionInput> getRightPartitioned()
    {
        return rightPartitioned;
    }

    public void setRightPartitioned(List<PartitionInput> rightPartitioned)
    {
        this.rightPartitioned = rightPartitioned;
    }

    public JoinInfo getJoinInfo()
    {
        return joinInfo;
    }

    public void setJoinInfo(JoinInfo joinInfo)
    {
        this.joinInfo = joinInfo;
    }

    public OutputInfo getOutput()
    {
        return output;
    }

    public void setOutput(OutputInfo output)
    {
        this.output = output;
    }

    public static class JoinInfo
    {
        /**
         * The column ids of the partition key columns.
         */
        private List<Integer> keyColumnIds;

        /**
         * The hash values to be processed by a hash join worker.
         */
        private List<Integer> hashValues;

        /**
         * Default constructor for Jackson.
         */
        public JoinInfo() { }

        public JoinInfo(List<Integer> keyColumnIds, List<Integer> hashValues)
        {
            this.keyColumnIds = keyColumnIds;
            this.hashValues = hashValues;
        }

        public List<Integer> getKeyColumnIds()
        {
            return keyColumnIds;
        }

        public void setKeyColumnIds(List<Integer> keyColumnIds)
        {
            this.keyColumnIds = keyColumnIds;
        }

        public List<Integer> getHashValues()
        {
            return hashValues;
        }

        public void setHashValues(List<Integer> hashValues)
        {
            this.hashValues = hashValues;
        }
    }
}
