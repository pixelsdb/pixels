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
package io.pixelsdb.pixels.invoker.lambda;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastChainJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author hank
 * @create 2022-05-15
 */
public class TestBroadcastChainJoinLambdaInvoker
{
    @Test
    public void testRegionNationSupplierLineitem() throws ExecutionException, InterruptedException
    {
        String regionFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"region\",\"columnFilters\":{}}";
        String nationFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"nation\",\"columnFilters\":{}}";
        String supplierFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"supplier\",\"columnFilters\":{}}";
        String lineitemFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";

        BroadcastChainJoinInput joinInput = new BroadcastChainJoinInput();
        joinInput.setQueryId(123456);

        List<BroadcastTableInfo> leftTables = new ArrayList<>();
        List<ChainJoinInfo> chainJoinInfos = new ArrayList<>();

        BroadcastTableInfo region = new BroadcastTableInfo();
        region.setColumnsToRead(new String[]{"r_regionkey", "r_name"});
        region.setKeyColumnIds(new int[]{0});
        region.setTableName("region");
        region.setBase(true);
        region.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/region/v-0-order/20230416153117_0.pxl", 0, 4)))));
        region.setFilter(regionFilter);
        leftTables.add(region);

        BroadcastTableInfo nation = new BroadcastTableInfo();
        nation.setColumnsToRead(new String[]{"n_nationkey", "n_name", "n_regionkey"});
        nation.setKeyColumnIds(new int[]{2});
        nation.setTableName("nation");
        nation.setBase(true);
        nation.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/nation/v-0-order/20230416135645_0.pxl", 0, 4)))));
        nation.setFilter(nationFilter);
        leftTables.add(nation);

        ChainJoinInfo chainJoinInfo0 = new ChainJoinInfo();
        chainJoinInfo0.setJoinType(JoinType.EQUI_INNER);
        chainJoinInfo0.setSmallProjection(new boolean[]{false, true});
        chainJoinInfo0.setLargeProjection(new boolean[]{true, true, false});
        chainJoinInfo0.setPostPartition(false);
        chainJoinInfo0.setSmallColumnAlias(new String[]{"r_name"});
        chainJoinInfo0.setLargeColumnAlias(new String[]{"n_nationkey", "n_name"});
        chainJoinInfo0.setKeyColumnIds(new int[]{1});
        chainJoinInfos.add(chainJoinInfo0);

        BroadcastTableInfo supplier = new BroadcastTableInfo();
        supplier.setColumnsToRead(new String[]{"s_suppkey", "s_name", "s_nationkey"});
        supplier.setKeyColumnIds(new int[]{2});
        supplier.setTableName("supplier");
        supplier.setBase(true);
        supplier.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/supplier/v-0-compact/20230416155327_0_compact.pxl", 0, 4)))));
        supplier.setFilter(supplierFilter);
        leftTables.add(supplier);

        ChainJoinInfo chainJoinInfo1 = new ChainJoinInfo();
        chainJoinInfo1.setJoinType(JoinType.EQUI_INNER);
        chainJoinInfo1.setSmallProjection(new boolean[]{true, false, true});
        chainJoinInfo1.setLargeProjection(new boolean[]{true, true, false});
        chainJoinInfo1.setPostPartition(false);
        chainJoinInfo1.setSmallColumnAlias(new String[]{"r_name", "n_name"});
        chainJoinInfo1.setLargeColumnAlias(new String[]{"s_suppkey", "s_name"});
        chainJoinInfo1.setKeyColumnIds(new int[]{2});
        chainJoinInfos.add(chainJoinInfo1);
        
        joinInput.setChainTables(leftTables);
        joinInput.setChainJoinInfos(chainJoinInfos);

        BroadcastTableInfo lineitem = new BroadcastTableInfo();
        lineitem.setColumnsToRead(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        lineitem.setKeyColumnIds(new int[]{1});
        lineitem.setTableName("lineitem");
        lineitem.setBase(true);
        lineitem.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/lineitem/v-0-compact/20230416153320_0_compact.pxl", 28, 4)))));
        lineitem.setFilter(lineitemFilter);
        joinInput.setLargeTable(lineitem);

        JoinInfo joinInfo = new JoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setSmallColumnAlias(new String[]{"r_name", "n_name", "s_name"});
        joinInfo.setLargeColumnAlias(new String[]{"l_orderkey", "l_extendedprice", "l_discount"});
        joinInfo.setSmallProjection(new boolean[]{true, true, false, true});
        joinInfo.setLargeProjection(new boolean[]{true, false, true, true});
        joinInfo.setPostPartition(true);
        joinInfo.setPostPartitionInfo(new PartitionInfo(new int[] {3}, 100));
        joinInput.setJoinInfo(joinInfo);

        joinInput.setOutput(new MultiOutputInfo("pixels-lambda-test/unit_tests/",
                new StorageInfo(Storage.Scheme.s3, null, null, null), true,
                Arrays.asList("broadcast_chain_join_0")));

        System.out.println(JSON.toJSONString(joinInput));
        JoinOutput output = (JoinOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.BROADCAST_CHAIN_JOIN).invoke(joinInput).get();
        System.out.println(output.getOutputs().size());
        for (int i = 0; i < output.getOutputs().size(); ++i)
        {
            System.out.println(output.getOutputs().get(i));
            System.out.println(output.getRowGroupNums().get(i));
            System.out.println();
        }
    }
}
