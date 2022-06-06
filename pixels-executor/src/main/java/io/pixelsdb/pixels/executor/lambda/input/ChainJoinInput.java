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

import io.pixelsdb.pixels.executor.lambda.domain.BroadCastJoinTableInfo;
import io.pixelsdb.pixels.executor.lambda.domain.ChainJoinInfo;
import io.pixelsdb.pixels.executor.lambda.domain.JoinInfo;
import io.pixelsdb.pixels.executor.lambda.domain.MultiOutputInfo;

import java.util.List;

/**
 * The input format of the chained broadcast join.
 *
 * @author hank
 * @date 03/06/2022
 */
public class ChainJoinInput implements JoinInput
{
    private long queryId;
    /**
     * The information of the small tables that are broadcast in the chain join.
     */
    private List<BroadCastJoinTableInfo> smallTables;
    /**
     * The information of the large table.
     */
    private BroadCastJoinTableInfo largeTable;
    /**
     * The information of the chain joins. If there is N left tables and 1 right table,
     * there should be N-1 chain join infos.
     */
    private List<ChainJoinInfo> chainJoinInfos;
    /**
     * The information of the last join with the right table.
     */
    private JoinInfo joinInfo;
    /**
     * The information of the join output files.<br/>
     * <b>Note: </b>for inner, right-outer, and natural joins, the number of output files
     * should be consistent with the number of input splits in right table. For left-outer
     * and full-outer joins, there is an additional output file for the left-outer records.
     */
    private MultiOutputInfo output;

    /**
     * Default constructor for Jackson.
     */
    public ChainJoinInput() { }

    public ChainJoinInput(long queryId, List<BroadCastJoinTableInfo> smallTables,
                          BroadCastJoinTableInfo largeTable, List<ChainJoinInfo> chainJoinInfos,
                          JoinInfo joinInfo, MultiOutputInfo output)
    {
        this.queryId = queryId;
        this.smallTables = smallTables;
        this.largeTable = largeTable;
        this.chainJoinInfos = chainJoinInfos;
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

    public List<BroadCastJoinTableInfo> getSmallTables()
    {
        return smallTables;
    }

    public void setSmallTables(List<BroadCastJoinTableInfo> smallTables)
    {
        this.smallTables = smallTables;
    }

    public BroadCastJoinTableInfo getLargeTable()
    {
        return largeTable;
    }

    public void setLargeTable(BroadCastJoinTableInfo largeTable)
    {
        this.largeTable = largeTable;
    }

    public List<ChainJoinInfo> getChainJoinInfos()
    {
        return chainJoinInfos;
    }

    public void setChainJoinInfos(List<ChainJoinInfo> chainJoinInfos)
    {
        this.chainJoinInfos = chainJoinInfos;
    }

    public JoinInfo getJoinInfo()
    {
        return joinInfo;
    }

    public void setJoinInfo(JoinInfo joinInfo)
    {
        this.joinInfo = joinInfo;
    }

    @Override
    public MultiOutputInfo getOutput()
    {
        return output;
    }

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
        private final ChainJoinInput builderInstance;

        private Builder(ChainJoinInput instance)
        {
            this.builderInstance = new ChainJoinInput(
                    instance.queryId, instance.smallTables, instance.largeTable,
                    instance.chainJoinInfos, instance.joinInfo, instance.output);
        }

        public Builder setLargeTable(BroadCastJoinTableInfo largeTable)
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

        public ChainJoinInput build()
        {
            return this.builderInstance;
        }
    }
}
