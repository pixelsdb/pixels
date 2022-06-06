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
package io.pixelsdb.pixels.executor;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.lambda.JoinOperator;
import io.pixelsdb.pixels.executor.plan.BaseTable;
import io.pixelsdb.pixels.executor.plan.Join;
import io.pixelsdb.pixels.executor.plan.JoinedTable;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

/**
 * @author hank
 * @date 06/06/2022
 */
public class TestLambdaJoinExecutor
{
    @Test
    public void testChainJoin() throws IOException, MetadataException
    {
        BaseTable region = new BaseTable(
                "tpch", "region", "region",
                new String[] {"r_regionkey", "r_name"},
                TableScanFilter.empty("tpch", "region"));

        BaseTable nation = new BaseTable(
                "tpch", "nation", "nation",
                new String[] {"n_nationkey", "n_name", "n_regionkey"},
                TableScanFilter.empty("tpch", "nation"));

        Join join1 = new Join(region, nation, new int[]{0}, new int[]{2},
                JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable1 = new JoinedTable(
                "join_1", "region_join_nation", "region_join_nation",
                new String[] {"r_name", "n_nationkey", "n_name"}, false, join1);

        BaseTable supplier = new BaseTable(
                "tpch", "supplier", "supplier",
                new String[] {"s_suppkey", "s_name", "s_acctbal", "s_nationkey"},
                TableScanFilter.empty("tpch", "supplier"));

        Join join2 = new Join(joinedTable1, supplier, new int[]{1}, new int[]{3},
                JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable2 = new JoinedTable(
                "join_2",
                "region_join_nation_join_supplier",
                "region_join_nation_join_supplier",
                new String[] {"r_name", "n_name", "s_suppkey", "s_name", "s_acctbal"},
                false, join2);

        BaseTable lineitem = new BaseTable(
                "tpch", "lineitem", "lineitem",
                new String[] {"l_orderkey", "l_suppkey", "l_extendedprice", "l_shipdate"},
                TableScanFilter.empty("tpch", "lineitem"));

        Join join3 = new Join(joinedTable2, lineitem, new int[]{2}, new int[]{1},
                JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable root = new JoinedTable("tpch",
                "region_join_nation_join_supplier_join_lineitem",
                "region_join_nation_join_supplier_join_lineitem",
                new String[]{"r_name", "n_name", "s_name", "s_acctbal", "l_orderkey", "l_extendedprice", "l_shipdate"},
                false, join3);

        LambdaJoinExecutor joinExecutor = new LambdaJoinExecutor(
                123456, root, false, true);

        JoinOperator joinOperator = joinExecutor.getJoinOperator(root, Optional.empty());
    }

    @Test
    public void testChainPartitionedBroadcastJoin() throws IOException, MetadataException
    {
        BaseTable region = new BaseTable(
                "tpch", "region", "region",
                new String[] {"r_regionkey", "r_name"},
                TableScanFilter.empty("tpch", "region"));

        BaseTable nation = new BaseTable(
                "tpch", "nation", "nation",
                new String[] {"n_nationkey", "n_name", "n_regionkey"},
                TableScanFilter.empty("tpch", "nation"));

        Join join1 = new Join(region, nation, new int[]{0}, new int[]{2},
                JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable1 = new JoinedTable(
                "join_1", "region_join_nation", "region_join_nation",
                new String[] {"r_name", "n_nationkey", "n_name"}, false, join1);

        BaseTable supplier = new BaseTable(
                "tpch", "supplier", "supplier",
                new String[] {"s_suppkey", "s_name", "s_acctbal", "s_nationkey"},
                TableScanFilter.empty("tpch", "supplier"));

        Join join2 = new Join(joinedTable1, supplier, new int[]{1}, new int[]{3},
                JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable2 = new JoinedTable(
                "join_2",
                "region_join_nation_join_supplier",
                "region_join_nation_join_supplier",
                new String[] {"r_name", "n_name", "s_suppkey", "s_name", "s_acctbal"},
                false, join2);

        BaseTable lineitem = new BaseTable(
                "tpch", "lineitem", "lineitem",
                new String[] {"l_orderkey", "l_suppkey", "l_extendedprice", "l_shipdate"},
                TableScanFilter.empty("tpch", "lineitem"));

        Join join3 = new Join(joinedTable2, lineitem, new int[]{2}, new int[]{1},
                JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable3 = new JoinedTable("tpch",
                "region_join_nation_join_supplier_join_lineitem",
                "region_join_nation_join_supplier_join_lineitem",
                new String[]{"r_name", "n_name", "s_name", "s_acctbal", "l_orderkey", "l_extendedprice", "l_shipdate"},
                false, join3);

        BaseTable orders = new BaseTable(
                "tpch", "orders", "orders",
                new String[] {"o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"},
                TableScanFilter.empty("tpch", "orders"));

        Join join4 = new Join(joinedTable3, orders, new int[]{4}, new int[]{0},
                JoinType.EQUI_INNER, JoinAlgorithm.PARTITIONED);

        JoinedTable joinedTable4 = new JoinedTable("tpch",
                "region_join_nation_join_supplier_join_lineitem_join_orders",
                "region_join_nation_join_supplier_join_lineitem_join_orders",
                new String[]{"r_name", "n_name", "s_name", "s_acctbal", "l_extendedprice", "l_shipdate",
                        "o_custkey", "o_orderdate", "o_totalprice"},
                false, join4);

        BaseTable customer = new BaseTable(
                "tpch", "customer", "customer",
                new String[] {"c_custkey", "c_name"},
                TableScanFilter.empty("tpch", "orders"));

        Join join5 = new Join(joinedTable4, customer, new int[]{6}, new int[]{0},
                JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable root = new JoinedTable("tpch",
                "region_join_nation_join_supplier_join_lineitem_join_orders_join_customer",
                "region_join_nation_join_supplier_join_lineitem_join_orders_join_customer",
                new String[]{"r_name", "n_name", "s_name", "s_acctbal", "l_extendedprice", "l_shipdate",
                        "o_orderdate", "o_totalprice", "c_name"},
                false, join5);

        LambdaJoinExecutor joinExecutor = new LambdaJoinExecutor(
                123456, root, false, true);

        JoinOperator joinOperator = joinExecutor.getJoinOperator(root, Optional.empty());
    }
}
