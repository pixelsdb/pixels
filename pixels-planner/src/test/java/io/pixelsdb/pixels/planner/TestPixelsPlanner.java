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
package io.pixelsdb.pixels.planner;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.Operator;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.logical.BaseTable;
import io.pixelsdb.pixels.planner.plan.logical.Join;
import io.pixelsdb.pixels.planner.plan.logical.JoinEndian;
import io.pixelsdb.pixels.planner.plan.logical.JoinedTable;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

/**
 * @author hank
 * @create 2022-06-06
 */
public class TestPixelsPlanner
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

        Join join1 = new Join(region, nation,
                new String[] {"r_name"}, new String[]{"n_nationkey", "n_name"},
                new int[]{0}, new int[]{2}, new boolean[]{false, true}, new boolean[]{true, true, false},
                JoinEndian.SMALL_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable1 = new JoinedTable(
                "join_1", "region_join_nation", "region_join_nation", join1);

        BaseTable supplier = new BaseTable(
                "tpch", "supplier", "supplier",
                new String[] {"s_suppkey", "s_name", "s_acctbal", "s_nationkey"},
                TableScanFilter.empty("tpch", "supplier"));

        Join join2 = new Join(joinedTable1, supplier,
                new String[] {"r_name", "n_name"},
                new String[] {"s_suppkey", "s_name", "s_acctbal"},
                new int[]{1}, new int[]{3}, new boolean[]{true, false, true},
                new boolean[]{true, true, true, false},
                JoinEndian.SMALL_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable2 = new JoinedTable(
                "join_2", "region_join_nation_join_supplier",
                "region_join_nation_join_supplier", join2);

        BaseTable lineitem = new BaseTable(
                "tpch", "lineitem", "lineitem",
                new String[] {"l_orderkey", "l_suppkey", "l_extendedprice", "l_shipdate"},
                TableScanFilter.empty("tpch", "lineitem"));

        Join join3 = new Join(joinedTable2, lineitem,
                new String[] {"r_name", "n_name", "s_name", "s_acctbal"},
                new String[] {"l_orderkey", "l_extendedprice", "l_shipdate"},
                new int[]{2}, new int[]{1}, new boolean[]{true, true, false, true, true},
                new boolean[]{true, false, true, true},
                JoinEndian.SMALL_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable root = new JoinedTable("tpch",
                "region_join_nation_join_supplier_join_lineitem",
                "region_join_nation_join_supplier_join_lineitem", join3);

        PixelsPlanner joinExecutor = new PixelsPlanner(
                123456, root, false, true, Optional.empty());

        Operator joinOperator = joinExecutor.getRootOperator();
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

        Join join1 = new Join(region, nation,
                new String[] {"r_name"}, new String[] {"n_nationkey", "n_name"},
                new int[]{0}, new int[]{2}, new boolean[]{false, true}, new boolean[]{true, true, false},
                JoinEndian.SMALL_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable1 = new JoinedTable(
                "join_1", "region_join_nation", "region_join_nation", join1);

        BaseTable supplier = new BaseTable(
                "tpch", "supplier", "supplier",
                new String[] {"s_suppkey", "s_name", "s_acctbal", "s_nationkey"},
                TableScanFilter.empty("tpch", "supplier"));

        Join join2 = new Join(joinedTable1, supplier,
                new String[] {"r_name", "n_name"},
                new String[] {"s_suppkey", "s_name", "s_acctbal"},
                new int[]{1}, new int[]{3}, new boolean[]{true, false, true},
                new boolean[]{true, true, true, false},
                JoinEndian.SMALL_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable2 = new JoinedTable(
                "join_2",
                "region_join_nation_join_supplier",
                "region_join_nation_join_supplier", join2);

        BaseTable lineitem = new BaseTable(
                "tpch", "lineitem", "lineitem",
                new String[] {"l_orderkey", "l_suppkey", "l_extendedprice", "l_shipdate"},
                TableScanFilter.empty("tpch", "lineitem"));

        Join join3 = new Join(joinedTable2, lineitem,
                new String[]{"r_name", "n_name", "s_name", "s_acctbal"},
                new String[]{"l_orderkey", "l_extendedprice", "l_shipdate"},
                new int[]{2}, new int[]{1}, new boolean[]{true, true, false, true, true},
                new boolean[]{true, false, true, true},
                JoinEndian.SMALL_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable3 = new JoinedTable("tpch",
                "region_join_nation_join_supplier_join_lineitem",
                "region_join_nation_join_supplier_join_lineitem", join3);

        BaseTable orders = new BaseTable(
                "tpch", "orders", "orders",
                new String[] {"o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"},
                TableScanFilter.empty("tpch", "orders"));

        Join join4 = new Join(joinedTable3, orders,
                new String[]{"r_name", "n_name", "s_name", "s_acctbal", "l_extendedprice", "l_shipdate"},
                new String[]{"o_custkey", "o_orderdate", "o_totalprice"},
                new int[]{4}, new int[]{0}, new boolean[]{true, true, true, true, false, true, true},
                new boolean[]{false, true, true, true},
                JoinEndian.LARGE_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.PARTITIONED);

        JoinedTable joinedTable4 = new JoinedTable("tpch",
                "region_join_nation_join_supplier_join_lineitem_join_orders",
                "region_join_nation_join_supplier_join_lineitem_join_orders", join4);

        BaseTable customer = new BaseTable(
                "tpch", "customer", "customer",
                new String[] {"c_custkey", "c_name"},
                TableScanFilter.empty("tpch", "orders"));

        Join join5 = new Join(joinedTable4, customer,
                new String[]{"r_name", "n_name", "s_name", "s_acctbal", "l_extendedprice", "l_shipdate",
                        "o_orderdate", "o_totalprice"}, new String[]{"c_name"},
                new int[]{6}, new int[]{0}, new boolean[]{true, true, true, true, true, true, false, true, true},
                new boolean[]{false, true}, JoinEndian.LARGE_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable root = new JoinedTable("tpch",
                "region_join_nation_join_supplier_join_lineitem_join_orders_join_customer",
                "region_join_nation_join_supplier_join_lineitem_join_orders_join_customer", join5);

        PixelsPlanner joinExecutor = new PixelsPlanner(
                123456, root, false, true, Optional.empty());

        Operator joinOperator = joinExecutor.getRootOperator();
    }

    @Test
    public void testChainBroadcastJoin() throws IOException, MetadataException
    {
        BaseTable region = new BaseTable(
                "tpch", "region", "region",
                new String[] {"r_regionkey", "r_name"},
                TableScanFilter.empty("tpch", "region"));

        BaseTable nation = new BaseTable(
                "tpch", "nation", "nation",
                new String[] {"n_nationkey", "n_name", "n_regionkey"},
                TableScanFilter.empty("tpch", "nation"));

        Join join1 = new Join(region, nation,
                new String[] {"r_name"}, new String[] {"n_nationkey", "n_name"},
                new int[]{0}, new int[]{2}, new boolean[]{false, true}, new boolean[]{true, true, false},
                JoinEndian.SMALL_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable1 = new JoinedTable(
                "join_1", "region_join_nation", "region_join_nation", join1);

        BaseTable supplier = new BaseTable(
                "tpch", "supplier", "supplier",
                new String[] {"s_suppkey", "s_name", "s_acctbal", "s_nationkey"},
                TableScanFilter.empty("tpch", "supplier"));

        Join join2 = new Join(joinedTable1, supplier,
                new String[] {"r_name", "n_name"},
                new String[] {"s_suppkey", "s_name", "s_acctbal"},
                new int[]{1}, new int[]{3}, new boolean[]{true, false, true}, new boolean[]{true, true, true, false},
                JoinEndian.SMALL_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable2 = new JoinedTable(
                "join_2",
                "region_join_nation_join_supplier",
                "region_join_nation_join_supplier", join2);

        BaseTable lineitem = new BaseTable(
                "tpch", "lineitem", "lineitem",
                new String[] {"l_partkey", "l_suppkey", "l_extendedprice", "l_shipdate"},
                TableScanFilter.empty("tpch", "lineitem"));

        Join join3 = new Join(joinedTable2, lineitem,
                new String[]{"r_name", "n_name", "s_name", "s_acctbal"},
                new String[]{"l_partkey", "l_extendedprice", "l_shipdate"},
                new int[]{2}, new int[]{1}, new boolean[]{true, true, false, true, true},
                new boolean[]{true, false, true, true},
                JoinEndian.SMALL_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable3 = new JoinedTable("tpch",
                "region_join_nation_join_supplier_join_lineitem",
                "region_join_nation_join_supplier_join_lineitem", join3);

        BaseTable part = new BaseTable(
                "tpch", "part", "part",
                new String[] {"p_partkey", "p_name", "p_type"},
                TableScanFilter.empty("tpch", "part"));

        Join join4 = new Join(joinedTable3, part,
                new String[]{"r_name", "n_name", "s_name", "s_acctbal", "l_extendedprice", "l_shipdate"},
                new String[]{"p_name", "p_type"},
                new int[]{4}, new int[]{0}, new boolean[]{true, true, true, true, false, true, true},
                new boolean[]{false, true, true}, JoinEndian.LARGE_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable root = new JoinedTable("tpch",
                "region_join_nation_join_supplier_join_lineitem_join_part",
                "region_join_nation_join_supplier_join_lineitem_join_part", join4);

        PixelsPlanner joinExecutor = new PixelsPlanner(
                123456, root, false, true, Optional.empty());

        Operator joinOperator = joinExecutor.getRootOperator();
    }
}
