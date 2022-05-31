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

import io.pixelsdb.pixels.executor.lambda.ScanInput.InputInfo;

import java.util.List;

/**
 * The input format for hash partitioning.
 * Hash partitioner is also responsible for projection and filtering, thus
 * hash partitioning input shares some fields with the scan input.
 *
 * @author hank
 * @date 07/05/2022
 */
public class PartitionInput
{
    /**
     * The unique id of the query.
     */
    private long queryId;
    /**
     * The information of the input files to be scanned.
     * rgLength in each input is the number of row groups to be scanned from each file.
     */
    private List<InputInfo> inputs;
    /**
     * The number of row groups to be scanned in each query split.
     */
    private int splitSize;
    /**
     * The description of the output folder where the scan results are written into.
     */
    private OutputInfo output;
    /**
     * The name of the columns to scan.
     */
    private String[] cols;
    /**
     * The json string of the filter (i.e., predicates) to be used in scan.
     */
    private String filter;

    /**
     * The information about the hash partitioning.
     */
    private PartitionInfo partitionInfo;

    /**
     * Default constructor for Jackson.
     */
    public PartitionInput() { }

    public PartitionInput(long queryId, List<InputInfo> inputs, int splitSize,
                     OutputInfo output, String[] cols, String filter, PartitionInfo partitionInfo)
    {
        this.queryId = queryId;
        this.inputs = inputs;
        this.splitSize = splitSize;
        this.output = output;
        this.cols = cols;
        this.filter = filter;
        this.partitionInfo  = partitionInfo;
    }

    public long getQueryId()
    {
        return queryId;
    }

    public void setQueryId(long queryId)
    {
        this.queryId = queryId;
    }

    public List<InputInfo> getInputs()
    {
        return inputs;
    }

    public void setInputs(List<InputInfo> inputs)
    {
        this.inputs = inputs;
    }

    public int getSplitSize()
    {
        return splitSize;
    }

    public void setSplitSize(int splitSize)
    {
        this.splitSize = splitSize;
    }

    public OutputInfo getOutput()
    {
        return output;
    }

    public void setOutput(OutputInfo output)
    {
        this.output = output;
    }

    public String[] getCols()
    {
        return cols;
    }

    public void setCols(String[] cols)
    {
        this.cols = cols;
    }

    public String getFilter()
    {
        return filter;
    }

    public void setFilter(String filter)
    {
        this.filter = filter;
    }

    public PartitionInfo getPartitionInfo()
    {
        return partitionInfo;
    }

    public void setPartitionInfo(PartitionInfo partitionInfo)
    {
        this.partitionInfo = partitionInfo;
    }

    public static class PartitionInfo
    {
        /**
         * The column ids of the partition key columns.
         */
        private int[] keyColumnIds;

        /**
         * The number of partitions in the output.
         */
        private int numParition;

        /**
         * Default constructor for Jackson.
         */
        public PartitionInfo() { }

        public PartitionInfo(int[] keyColumnIds, int numParition)
        {
            this.keyColumnIds = keyColumnIds;
            this.numParition = numParition;
        }

        public int[] getKeyColumnIds()
        {
            return keyColumnIds;
        }

        public void setKeyColumnIds(int[] keyColumnIds)
        {
            this.keyColumnIds = keyColumnIds;
        }

        public int getNumParition()
        {
            return numParition;
        }

        public void setNumParition(int numParition)
        {
            this.numParition = numParition;
        }
    }

    public static class OutputInfo
    {
        /**
         * The path of the partitioned file.
         */
        private String path;

        /**
         * Whether we use encoding for the partitioned file.
         */
        private boolean encoding;

        /**
         * Default constructor for Jackson.
         */
        public OutputInfo() {}

        public OutputInfo(String path, boolean encoding)
        {
            this.path = path;
            this.encoding = encoding;
        }

        public String getPath()
        {
            return path;
        }

        public void setPath(String path)
        {
            this.path = path;
        }

        public boolean isEncoding()
        {
            return encoding;
        }

        public void setEncoding(boolean encoding)
        {
            this.encoding = encoding;
        }
    }
}
