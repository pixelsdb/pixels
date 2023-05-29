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
 * The input format of the chained broadcast join.
 *
 * @author hank
 * @create 2022-06-03
 */
public class BroadcastChainJoinInput extends JoinInput
{
    /**
     * The information of the chain tables that are broadcast in the chain join.
     */
    private List<BroadcastTableInfo> chainTables;
    /**
     * The information of the chain joins. If there are N chain tables and 1 right table,
     * there should be N-1 chain join infos.
     */
    private List<ChainJoinInfo> chainJoinInfos;
    /**
     * The information of the large table.
     */
    private BroadcastTableInfo largeTable;
    /**
     * The information of the last join with the right table.
     */
    private JoinInfo joinInfo;
    /**
     * Whether there are post chain joins.
     */
    protected boolean postChainJoinsPresent = false;
    /**
     * The information of the post small tables that are broadcast in the chain join.
     */
    private List<BroadcastTableInfo> postSmallTables;
    /**
     * The information of the post chain joins. If there is M post small tables,
     * there should be M post chain join infos.
     */
    private List<ChainJoinInfo> postChainJoinInfos;

    /**
     * Default constructor for Jackson.
     */
    public BroadcastChainJoinInput() { }

    public BroadcastChainJoinInput(long transId, List<BroadcastTableInfo> chainTables, List<ChainJoinInfo> chainJoinInfos,
                                   BroadcastTableInfo largeTable, JoinInfo joinInfo, boolean postChainJoinsPresent,
                                   List<BroadcastTableInfo> postSmallTables, List<ChainJoinInfo> postChainJoinInfos,
                                   boolean partialAggregationPresent, PartialAggregationInfo partialAggregationInfo,
                                   MultiOutputInfo output)
    {
        super(transId, partialAggregationPresent, partialAggregationInfo, output);
        this.chainTables = chainTables;
        this.chainJoinInfos = chainJoinInfos;
        this.largeTable = largeTable;
        this.joinInfo = joinInfo;
        this.postChainJoinsPresent = postChainJoinsPresent;
        this.postSmallTables = postSmallTables;
        this.postChainJoinInfos = postChainJoinInfos;
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

    public BroadcastTableInfo getLargeTable()
    {
        return largeTable;
    }

    public void setLargeTable(BroadcastTableInfo largeTable)
    {
        this.largeTable = largeTable;
    }

    public JoinInfo getJoinInfo()
    {
        return joinInfo;
    }

    public void setJoinInfo(JoinInfo joinInfo)
    {
        this.joinInfo = joinInfo;
    }

    public boolean isPostChainJoinsPresent()
    {
        return postChainJoinsPresent;
    }

    public void setPostChainJoinsPresent(boolean postChainJoinsPresent)
    {
        this.postChainJoinsPresent = postChainJoinsPresent;
    }

    public List<BroadcastTableInfo> getPostSmallTables()
    {
        return postSmallTables;
    }

    public void setPostSmallTables(List<BroadcastTableInfo> postSmallTables)
    {
        this.postSmallTables = postSmallTables;
    }

    public List<ChainJoinInfo> getPostChainJoinInfos()
    {
        return postChainJoinInfos;
    }

    public void setPostChainJoinInfos(List<ChainJoinInfo> postChainJoinInfos)
    {
        this.postChainJoinInfos = postChainJoinInfos;
    }

    public Builder toBuilder()
    {
        return new Builder(this);
    }

    public static class Builder
    {
        private final BroadcastChainJoinInput builderInstance;

        private Builder(BroadcastChainJoinInput instance)
        {
            this.builderInstance = new BroadcastChainJoinInput(
                    instance.getTransId(), instance.chainTables, instance.chainJoinInfos, instance.largeTable,
                    instance.joinInfo, instance.postChainJoinsPresent, instance.postSmallTables, instance.postChainJoinInfos,
                    instance.isPartialAggregationPresent(), instance.getPartialAggregationInfo(), instance.getOutput());
        }

        public Builder setLargeTable(BroadcastTableInfo largeTable)
        {
            this.builderInstance.setLargeTable(largeTable);
            return this;
        }

        public Builder setJoinInfo(JoinInfo joinInfo)
        {
            this.builderInstance.setJoinInfo(joinInfo);
            return this;
        }

        public Builder setOutput(MultiOutputInfo output)
        {
            this.builderInstance.setOutput(output);
            return this;
        }

        public BroadcastChainJoinInput build()
        {
            return this.builderInstance;
        }
    }
}
