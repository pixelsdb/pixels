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
package io.pixelsdb.pixels.executor.lambda.input;

import io.pixelsdb.pixels.executor.lambda.domain.*;

import java.util.List;

/**
 * The input format of the chained partitioned join.
 * @author hank
 * @date 25/06/2022
 */
public class PartitionedChainJoinInput implements JoinInput
{
    /**
     * The unique id of the query.
     */
    private long queryId;
    /**
     * The information of the chain tables that are broadcast in the chain join.
     */
    private List<BroadCastJoinTableInfo> chainTables;
    /**
     * The information of the chain joins. If there are N chain tables and 1 right table,
     * there should be N-1 chain join infos.
     */
    private List<ChainJoinInfo> chainJoinInfos;
    /**
     * The information of the small partitioned table.
     */
    private PartitionedTableInfo smallTable;
    /**
     * The information of the large partitioned table.
     */
    private PartitionedTableInfo largeTable;
    /**
     * The information of the partitioned join.
     */
    private PartitionedJoinInfo joinInfo;
    /**
     * The information of the join output files.<br/>
     * <b>Note: </b>for inner, right-outer, and natural joins, the number of output files
     * should be consistent with the parallelism of the right table. For left-outer and
     * full-outer joins, there is an additional output file for the left-outer records.
     */
    private MultiOutputInfo output;

    /**
     * Default constructor for Jackson.
     */
    public PartitionedChainJoinInput() { }

    public PartitionedChainJoinInput(long queryId,
                                     List<BroadCastJoinTableInfo> chainTables,
                                     List<ChainJoinInfo> chainJoinInfos,
                                     PartitionedTableInfo smallTable,
                                     PartitionedTableInfo largeTable,
                                     PartitionedJoinInfo joinInfo,
                                     MultiOutputInfo output)
    {
        this.queryId = queryId;
        this.chainTables = chainTables;
        this.chainJoinInfos = chainJoinInfos;
        this.smallTable = smallTable;
        this.largeTable = largeTable;
        this.joinInfo = joinInfo;
        this.output = output;
    }

    public long getQueryId()
    {
        return queryId;
    }

    public void setQueryId(long queryId)
    {
        this.queryId = queryId;
    }

    public List<BroadCastJoinTableInfo> getChainTables()
    {
        return chainTables;
    }

    public void setChainTables(List<BroadCastJoinTableInfo> chainTables)
    {
        this.chainTables = chainTables;
    }

    public List<ChainJoinInfo> getChainJoinInfos()
    {
        return chainJoinInfos;
    }

    public void setChainJoinInfos(List<ChainJoinInfo> chainJoinInfos)
    {
        this.chainJoinInfos = chainJoinInfos;
    }

    public PartitionedTableInfo getSmallTable()
    {
        return smallTable;
    }

    public void setSmallTable(PartitionedTableInfo smallTable)
    {
        this.smallTable = smallTable;
    }

    public PartitionedTableInfo getLargeTable()
    {
        return largeTable;
    }

    public void setLargeTable(PartitionedTableInfo largeTable)
    {
        this.largeTable = largeTable;
    }

    public PartitionedJoinInfo getJoinInfo()
    {
        return joinInfo;
    }

    public void setJoinInfo(PartitionedJoinInfo joinInfo)
    {
        this.joinInfo = joinInfo;
    }

    @Override
    public MultiOutputInfo getOutput()
    {
        return output;
    }

    @Override
    public void setOutput(MultiOutputInfo output)
    {
        this.output = output;
    }

    public Builder toBuilder()
    {
        return new Builder(this);
    }

    public static class Builder
    {
        private final PartitionedChainJoinInput builderInstance;

        private Builder(PartitionedChainJoinInput instance)
        {
            this.builderInstance = new PartitionedChainJoinInput(
                    instance.queryId, instance.chainTables, instance.chainJoinInfos,
                    instance.smallTable, instance.largeTable, instance.joinInfo, instance.output);
        }

        public Builder setLargeTable(PartitionedTableInfo largeTable)
        {
            this.builderInstance.setLargeTable(largeTable);
            return this;
        }

        public Builder setJoinInfo(PartitionedJoinInfo joinInfo)
        {
            this.builderInstance.setJoinInfo(joinInfo);
            return this;
        }

        public Builder setOutput(MultiOutputInfo output)
        {
            this.builderInstance.setOutput(output);
            return this;
        }

        public PartitionedChainJoinInput build()
        {
            return this.builderInstance;
        }
    }
}
