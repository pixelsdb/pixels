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
package io.pixelsdb.pixels.planner.plan.physical.input;

import io.pixelsdb.pixels.planner.plan.physical.domain.*;

import java.util.List;

/**
 * The input format of the chained partitioned join.
 * @author hank
 * @create 2022-06-25
 */
public class PartitionedChainJoinInput extends JoinInput
{
    /**
     * The information of the chain tables that are broadcast in the chain join.
     */
    private List<BroadcastTableInfo> chainTables;
    /**
     * The information of the chain joins. If there are N chain tables, there should be N
     * chain join infos.
     *
     * The last chain join info is for the final join of the chain tables and the join
     * result of the small and large partitioned tables. Its keyColumnIds is not the key
     * column ids of its join result, it is the key column ids of the join result of the
     * small and large partitioned tables.
     *
     * However, the post partitioning info of the last chain join is the post partitioning
     * info of the entire partitioned chain join.
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
     * The information of the partitioned join. Currently, the join type of the partitioned
     * join in a partitioned chain join <b>CAN NOT</b> be LEFT_OUTER or FULL_OUTER.
     *
     * TODO: support left/full outer join for partitioned chain join.
     */
    private PartitionedJoinInfo joinInfo;

    /**
     * Default constructor for Jackson.
     */
    public PartitionedChainJoinInput() { }

    public PartitionedChainJoinInput(long transId, List<BroadcastTableInfo> chainTables, List<ChainJoinInfo> chainJoinInfos,
                                     PartitionedTableInfo smallTable, PartitionedTableInfo largeTable,
                                     PartitionedJoinInfo joinInfo, boolean partialAggregationPresent,
                                     PartialAggregationInfo partialAggregationInfo, MultiOutputInfo output)
    {
        super(transId, partialAggregationPresent, partialAggregationInfo, output);
        this.chainTables = chainTables;
        this.chainJoinInfos = chainJoinInfos;
        this.smallTable = smallTable;
        this.largeTable = largeTable;
        this.joinInfo = joinInfo;
    }

    public List<BroadcastTableInfo> getChainTables()
    {
        return chainTables;
    }

    public void setChainTables(List<BroadcastTableInfo> chainTables)
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
                    instance.getTransId(), instance.chainTables, instance.chainJoinInfos, instance.smallTable,
                    instance.largeTable, instance.joinInfo, instance.isPartialAggregationPresent(),
                    instance.getPartialAggregationInfo(), instance.getOutput());
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
