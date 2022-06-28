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
package io.pixelsdb.pixels.executor.join;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.executor.lambda.InvokerFactory;
import io.pixelsdb.pixels.executor.lambda.WorkerType;
import io.pixelsdb.pixels.executor.lambda.domain.*;
import io.pixelsdb.pixels.executor.lambda.input.PartitionedChainJoinInput;
import io.pixelsdb.pixels.executor.lambda.output.JoinOutput;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author hank
 * @date 14/05/2022
 */
public class TestPartitionedChainJoinInvoker
{
    @Test
    public void testRegionNationSupplierOrdersLineitem() throws ExecutionException, InterruptedException
    {
        String regionFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"region\",\"columnFilters\":{}}";
        String nationFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"nation\",\"columnFilters\":{}}";
        String supplierFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"supplier\",\"columnFilters\":{}}";

        PartitionedChainJoinInput joinInput = new PartitionedChainJoinInput();
        joinInput.setQueryId(123456);

        List<BroadcastTableInfo> chainTables = new ArrayList<>();
        List<ChainJoinInfo> chainJoinInfos = new ArrayList<>();

        BroadcastTableInfo region = new BroadcastTableInfo();
        region.setColumnsToRead(new String[]{"r_regionkey", "r_name"});
        region.setKeyColumnIds(new int[]{0});
        region.setTableName("region");
        region.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/region/v-0-order/20220313093112_0.pxl", 0, 4)))));
        region.setFilter(regionFilter);
        chainTables.add(region);

        BroadcastTableInfo nation = new BroadcastTableInfo();
        nation.setColumnsToRead(new String[]{"n_nationkey", "n_name", "n_regionkey"});
        nation.setKeyColumnIds(new int[]{2});
        nation.setTableName("nation");
        nation.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("pixels-tpch/nation/v-0-order/20220313080937_0.pxl", 0, 4)))));
        nation.setFilter(nationFilter);
        chainTables.add(nation);

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
        supplier.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo(
                        "pixels-tpch/supplier/v-0-compact/20220313101902_0.compact.pxl",
                        0, 4)))));
        supplier.setFilter(supplierFilter);
        chainTables.add(supplier);

        ChainJoinInfo chainJoinInfo1 = new ChainJoinInfo();
        chainJoinInfo1.setJoinType(JoinType.EQUI_INNER);
        chainJoinInfo1.setSmallProjection(new boolean[]{true, false, true});
        chainJoinInfo1.setLargeProjection(new boolean[]{true, true, false});
        chainJoinInfo1.setPostPartition(false);
        chainJoinInfo1.setSmallColumnAlias(new String[]{"r_name", "n_name"});
        chainJoinInfo1.setLargeColumnAlias(new String[]{"s_suppkey", "s_name"});
        chainJoinInfo1.setKeyColumnIds(new int[]{2});
        chainJoinInfos.add(chainJoinInfo1);

        Set<Integer> hashValues = new HashSet<>(40);
        for (int i = 0 ; i < 40; ++i)
        {
            hashValues.add(i);
        }

        PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
        leftTableInfo.setTableName("orders");
        leftTableInfo.setColumnsToRead(new String[]
                {"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        leftTableInfo.setKeyColumnIds(new int[]{0});
        leftTableInfo.setInputFiles(Arrays.asList(
                "pixels-lambda-test/orders_part_0",
                "pixels-lambda-test/orders_part_1",
                "pixels-lambda-test/orders_part_2",
                "pixels-lambda-test/orders_part_3",
                "pixels-lambda-test/orders_part_4",
                "pixels-lambda-test/orders_part_5",
                "pixels-lambda-test/orders_part_6",
                "pixels-lambda-test/orders_part_7"));
        leftTableInfo.setParallelism(8);
        joinInput.setSmallTable(leftTableInfo);

        PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
        rightTableInfo.setTableName("lineitem");
        rightTableInfo.setColumnsToRead(new String[]
                {"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        rightTableInfo.setKeyColumnIds(new int[]{0});
        rightTableInfo.setInputFiles(Arrays.asList(
                "pixels-lambda-test/lineitem_part_0",
                "pixels-lambda-test/lineitem_part_1"));
        rightTableInfo.setParallelism(2);
        joinInput.setLargeTable(rightTableInfo);

        PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setNumPartition(40);
        joinInfo.setHashValues(Arrays.asList(16));
        joinInfo.setSmallColumnAlias(new String[]{"o_custkey", "o_orderstatus", "o_orderdate"});
        joinInfo.setLargeColumnAlias(new String[]{"l_suppkey", "l_extendedprice", "l_discount"});
        joinInfo.setSmallProjection(new boolean[]{false, true, true, true});
        joinInfo.setLargeProjection(new boolean[]{false, true, true, true});
        joinInfo.setPostPartition(false);
        joinInput.setJoinInfo(joinInfo);

        ChainJoinInfo chainJoinInfo2 = new ChainJoinInfo();
        chainJoinInfo2.setJoinType(JoinType.EQUI_INNER);
        chainJoinInfo2.setSmallProjection(new boolean[]{true, true, false, true});
        chainJoinInfo2.setLargeProjection(new boolean[]{true, true, true, false, true, true});
        chainJoinInfo2.setPostPartition(true);
        chainJoinInfo2.setPostPartitionInfo(new PartitionInfo(new int[] {3}, 20));
        chainJoinInfo2.setSmallColumnAlias(new String[]{"r_name", "n_name", "s_name"});
        chainJoinInfo2.setLargeColumnAlias(new String[]{
                "o_custkey", "o_orderstatus", "o_orderdate", "l_extendedprice", "l_discount"});
        chainJoinInfo2.setKeyColumnIds(new int[]{3});
        chainJoinInfos.add(chainJoinInfo2);

        joinInput.setChainTables(chainTables);
        joinInput.setChainJoinInfos(chainJoinInfos);

        joinInput.setOutput(new MultiOutputInfo("pixels-lambda-test/", Storage.Scheme.s3,
                null, null, null, true,
                Arrays.asList("partitioned-chain-join-0", "partitioned-chain-join-1")));

        System.out.println(JSON.toJSONString(joinInput));
        JoinOutput output = (JoinOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.PARTITIONED_CHAIN_JOIN).invoke(joinInput).get();
        System.out.println(output.getOutputs().size());
        for (int i = 0; i < output.getOutputs().size(); ++i)
        {
            System.out.println(output.getOutputs().get(i));
            System.out.println(output.getRowGroupNums().get(i));
            System.out.println();
        }
    }
}
