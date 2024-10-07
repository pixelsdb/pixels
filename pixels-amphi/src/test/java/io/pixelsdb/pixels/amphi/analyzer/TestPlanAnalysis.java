/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.amphi.analyzer;

import io.pixelsdb.pixels.amphi.TpchQuery;
import io.pixelsdb.pixels.common.exception.AmphiException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.parser.PixelsParser;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPlanAnalysis
{
    String hostAddr = "ec2-3-142-249-61.us-east-2.compute.amazonaws.com"; // 128g r5

//    String hostAddr = "ec2-18-218-128-203.us-east-2.compute.amazonaws.com"; // 8g t2
    MetadataService instance = null;

    PixelsParser tpchPixelsParser = null;
    PixelsParser clickbenchPixelsParser = null;

    @Before
    public void init()
    {
        this.instance = MetadataService.CreateInstance(hostAddr, 18888);
        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL_ANSI)
                .setParserFactory(SqlParserImpl.FACTORY)
                .build();

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

        this.tpchPixelsParser = new PixelsParser(this.instance, "tpch", parserConfig, properties);
        this.clickbenchPixelsParser = new PixelsParser(this.instance, "clickbench", parserConfig, properties);
    }

    @Test
    public void testPlanAnalysisSimple() throws SqlParseException
    {
        String query = "select SUM(o_totalprice) from orders, customer";

        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(query);
        System.out.println("Parsed SQL Query: \n" + parsedNode);

        SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);
        System.out.println("No exception, validation success.");

        RelNode rel = this.tpchPixelsParser.toRelNode(validatedNode);
        final RelJsonWriter writer = new RelJsonWriter();
        rel.explain(writer);
        System.out.println("Logical plan: \n" + writer.asString());

        // Run one-time traversal to collect analysis
        PlanAnalysis analysis = new PlanAnalysis(instance, query, rel, "tpch");
        analysis.traversePlan();

        // Get analyzed factors
        int nodeCount = analysis.getNodeCount();
        int maxDepth = analysis.getMaxDepth();
        int scannedTablesCount = analysis.getScannedTableCount();
        Set<String> operatorTypes = analysis.getOperatorTypes();
        Set<String> scannedTables = analysis.getScannedTables();
        List<Map<String, Object>> filterDetails = analysis.getFilterDetails();
        List<Map<String, Object>> joinDetails = analysis.getJoinDetails();
        List<Map<String, Object>> aggregateDetails = analysis.getAggregateDetails();

        System.out.println("Total number of nodes in plan: " + nodeCount);
        System.out.println("Maximum depth of the plan tree: " + maxDepth);
        System.out.println("Number of unique scanned tables: " + scannedTablesCount);
        System.out.println("Logical operators in plan: " + operatorTypes);
        System.out.println("Scanned tables in plan: " + scannedTables);
        System.out.println("Filter operators in plan: " + filterDetails);
        System.out.println("Join operators in plan: " + joinDetails);
        System.out.println("Aggregate operators in plan: " + aggregateDetails);

        // Validate the analysis
        int expectedNodeCount = 5;
        int expectedMaxDepth = 4;
        int expectedScannedTablesCount = 2;
        Set<String> expectedOperators = new HashSet<String>() {{
            add("LogicalAggregate");
            add("LogicalJoin");
            add("LogicalTableScan");
            add("LogicalProject");
        }};
        Set<String> expectedScannedTables = new HashSet<String>() {{
            add("orders");
            add("customer");
        }};
        List<Map<String, Object>> expectedFilterDetails = new ArrayList<>();
        List<Map<String, Object>> expectedJoinDetails = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("LeftTable", "orders");
                put("JoinType", JoinRelType.INNER);
                put("RightTable", "customer");
                put("JoinCondition", "true");
            }});
        }};
        List<Map<String, Object>> expectedAggregateDetails = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("AggregateFunctions", Collections.singletonList("SUM"));
                put("GroupedFields", Collections.emptyList());
            }});
        }};

        assertEquals("4 RelNode in the logical plan", expectedNodeCount, nodeCount);
        assertEquals("3 maximum depth of the tree structure", expectedMaxDepth, maxDepth);
        assertEquals("2 unique table scanned", expectedScannedTablesCount, scannedTablesCount);
        assertEquals("3 Logical operators included in plan.", expectedOperators, operatorTypes);
        assertEquals("table orders and customer scanned by the plan.", expectedScannedTables, scannedTables);
        assertEquals("Filter operators in the plan have the expected details.", expectedFilterDetails, filterDetails);
        assertEquals("Join operators in the plan have the expected details.", expectedJoinDetails, joinDetails);
        assertEquals("Aggregate operators in the plan have the expected details.", expectedAggregateDetails, aggregateDetails);
    }

    @Test
    public void testPlanAnalysisColumnSimple() throws SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        String query = "select SUM(o_totalprice) from orders, customer";

        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(query);
        System.out.println("Parsed SQL Query: \n" + parsedNode);

        SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);
        System.out.println("No exception, validation success.");

        RelNode rel = this.tpchPixelsParser.toRelNode(validatedNode);

        // Run one-time traversal to collect analysis
        PlanAnalysis analysis = new PlanAnalysis(instance, query, rel, "tpch");
        analysis.analyze();

        System.out.println(analysis.getProjectColumns());
    }

    @Test
    public void testPlanAnalysisColumnTpch() throws SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        String query = "select o_year, sum( case when nation = 'INDIA' then volume else 0 end) / sum(volume) as mkt_share " +
                "from( select extract( year from o_orderdate ) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation " +
                "from PART, SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION n1, NATION n2, REGION " +
                "where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and " +
                "r_name = 'ASIA' and s_nationkey = n2.n_nationkey and o_orderdate between '1995-01-01' and '1996-12-31' and p_type = 'SMALL PLATED COPPER' ) as all_nations " +
                "group by o_year " +
                "order by o_year";

        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(query);
        System.out.println("Parsed SQL Query: \n" + parsedNode);

        SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);
        System.out.println("No exception, validation success.");

        RelNode rel = this.tpchPixelsParser.toRelNode(validatedNode);

        // Run one-time traversal to collect analysis
        PlanAnalysis analysis = new PlanAnalysis(instance, query, rel, "tpch");
        analysis.analyze();

        System.out.println(analysis.getProjectColumns());
    }

    @Test
    public void testPlanAnalysisColumnTpchQ1()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ1 = new HashMap<String, List<String>>()
        {{
            put("lineitem", new ArrayList<String>() {{
                add("l_returnflag");
                add("l_linestatus");
                add("l_quantity");
                add("l_extendedprice");
                add("l_discount");
                add("l_tax");
                add("l_shipdate");
            }});
        }};

        Map<String, List<String>> columnQ1 = deriveColumns(TpchQuery.getQuery(1), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ1, columnQ1);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ2()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ2 = new HashMap<String, List<String>>()
        {{
            put("part", new ArrayList<String>() {{
                add("p_partkey");
                add("p_size");
                add("p_type");
                add("p_mfgr");
            }});
            put("supplier", new ArrayList<String>() {{
                add("s_acctbal");
                add("s_name");
                add("s_suppkey");
                add("s_address");
                add("s_phone");
                add("s_comment");
                add("s_nationkey");
            }});
            put("partsupp", new ArrayList<String>() {{
                add("ps_partkey");
                add("ps_suppkey");
                add("ps_supplycost");
            }});
            put("nation", new ArrayList<String>() {{
                add("n_name");
                add("n_nationkey");
                add("n_regionkey");
            }});
            put("region", new ArrayList<String>() {{
                add("r_regionkey");
                add("r_name");
            }});
        }};

        Map<String, List<String>> columnQ2 = deriveColumns(TpchQuery.getQuery(2), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ2, columnQ2);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ3()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ3 = new HashMap<String, List<String>>()
        {{
            put("customer", new ArrayList<String>() {{
                add("c_mktsegment");
                add("c_custkey");
            }});
            put("orders", new ArrayList<String>() {{
                add("o_orderdate");
                add("o_shippriority");
                add("o_custkey");
                add("o_orderkey");
            }});
            put("lineitem", new ArrayList<String>() {{
                add("l_orderkey");
                add("l_extendedprice");
                add("l_discount");
                add("l_shipdate");
            }});
        }};

        Map<String, List<String>> columnQ3 = deriveColumns(TpchQuery.getQuery(3), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ3, columnQ3);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ4()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ4 = new HashMap<String, List<String>>()
        {{
            put("orders", new ArrayList<String>() {{
                add("o_orderdate");
                add("o_orderpriority");
                add("o_orderkey");
            }});
            put("lineitem", new ArrayList<String>() {{
                add("l_orderkey");
                add("l_commitdate");
                add("l_receiptdate");
            }});
        }};

        Map<String, List<String>> columnQ4 = deriveColumns(TpchQuery.getQuery(4), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ4, columnQ4);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ5()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ5 = new HashMap<String, List<String>>()
        {{
            put("customer", new ArrayList<String>() {{
                add("c_custkey");
                add("c_nationkey");
            }});
            put("orders", new ArrayList<String>() {{
                add("o_orderkey");
                add("o_orderdate");
                add("o_custkey");
            }});
            put("lineitem", new ArrayList<String>() {{
                add("l_orderkey");
                add("l_extendedprice");
                add("l_discount");
                add("l_suppkey");
            }});
            put("supplier", new ArrayList<String>() {{
                add("s_suppkey");
                add("s_nationkey");
            }});
            put("nation", new ArrayList<String>() {{
                add("n_name");
                add("n_nationkey");
                add("n_regionkey");
            }});
            put("region", new ArrayList<String>() {{
                add("r_regionkey");
                add("r_name");
            }});
        }};

        Map<String, List<String>> columnQ5 = deriveColumns(TpchQuery.getQuery(5), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ5, columnQ5);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ6()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ6 = new HashMap<String, List<String>>()
        {{
            put("lineitem", new ArrayList<String>() {{
                add("l_extendedprice");
                add("l_discount");
                add("l_shipdate");
                add("l_quantity");
            }});
        }};

        Map<String, List<String>> columnQ6 = deriveColumns(TpchQuery.getQuery(6), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ6, columnQ6);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ7()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ7 = new HashMap<String, List<String>>()
        {{
            put("supplier", new ArrayList<String>() {{
                add("s_suppkey");
                add("s_nationkey");
            }});
            put("lineitem", new ArrayList<String>() {{
                add("l_shipdate");
                add("l_extendedprice");
                add("l_discount");
                add("l_suppkey");
                add("l_orderkey");
            }});
            put("orders", new ArrayList<String>() {{
                add("o_orderkey");
                add("o_custkey");
            }});
            put("customer", new ArrayList<String>() {{
                add("c_custkey");
                add("c_nationkey");
            }});
            put("nation", new ArrayList<String>() {{
                add("n_nationkey");
                add("n_name");
            }});
        }};

        Map<String, List<String>> columnQ7 = deriveColumns(TpchQuery.getQuery(7), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ7, columnQ7);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ8()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ8 = new HashMap<String, List<String>>()
        {{
            put("part", new ArrayList<String>() {{
                add("p_partkey");
                add("p_type");
            }});
            put("supplier", new ArrayList<String>() {{
                add("s_suppkey");
                add("s_nationkey");
            }});
            put("lineitem", new ArrayList<String>() {{
                add("l_extendedprice");
                add("l_discount");
                add("l_suppkey");
                add("l_orderkey");
                add("l_partkey");
            }});
            put("orders", new ArrayList<String>() {{
                add("o_orderkey");
                add("o_orderdate");
                add("o_custkey");
            }});
            put("customer", new ArrayList<String>() {{
                add("c_custkey");
                add("c_nationkey");
            }});
            put("nation", new ArrayList<String>() {{
                add("n_nationkey");
                add("n_name");
                add("n_regionkey");
            }});
            put("region", new ArrayList<String>() {{
                add("r_regionkey");
                add("r_name");
            }});
        }};

        Map<String, List<String>> columnQ8 = deriveColumns(TpchQuery.getQuery(8), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ8, columnQ8);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ9()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ9 = new HashMap<String, List<String>>()
        {{
            put("part", new ArrayList<String>() {{
                add("p_partkey");
                add("p_name");
            }});
            put("supplier", new ArrayList<String>() {{
                add("s_suppkey");
                add("s_nationkey");
            }});
            put("lineitem", new ArrayList<String>() {{
                add("l_extendedprice");
                add("l_discount");
                add("l_quantity");
                add("l_suppkey");
                add("l_orderkey");
                add("l_partkey");
            }});
            put("partsupp", new ArrayList<String>() {{
                add("ps_suppkey");
                add("ps_partkey");
                add("ps_supplycost");
            }});
            put("orders", new ArrayList<String>() {{
                add("o_orderkey");
                add("o_orderdate");
            }});
            put("nation", new ArrayList<String>() {{
                add("n_nationkey");
                add("n_name");
            }});
        }};

        Map<String, List<String>> columnQ9 = deriveColumns(TpchQuery.getQuery(9), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ9, columnQ9);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ10()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ10 = new HashMap<String, List<String>>()
        {{
            put("customer", new ArrayList<String>() {{
                add("c_custkey");
                add("c_name");
                add("c_acctbal");
                add("c_phone");
                add("c_address");
                add("c_comment");
                add("c_nationkey");
            }});
            put("orders", new ArrayList<String>() {{
                add("o_custkey");
                add("o_orderkey");
                add("o_orderdate");
            }});
            put("lineitem", new ArrayList<String>() {{
                add("l_orderkey");
                add("l_extendedprice");
                add("l_discount");
                add("l_returnflag");
            }});
            put("nation", new ArrayList<String>() {{
                add("n_nationkey");
                add("n_name");
            }});
        }};

        Map<String, List<String>> columnQ10 = deriveColumns(TpchQuery.getQuery(10), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ10, columnQ10);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ11()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ11 = new HashMap<String, List<String>>()
        {{
            put("partsupp", new ArrayList<String>() {{
                add("ps_partkey");
                add("ps_supplycost");
                add("ps_availqty");
                add("ps_suppkey");
            }});
            put("supplier", new ArrayList<String>() {{
                add("s_suppkey");
                add("s_nationkey");
            }});
            put("nation", new ArrayList<String>() {{
                add("n_nationkey");
                add("n_name");
            }});
        }};

        Map<String, List<String>> columnQ11 = deriveColumns(TpchQuery.getQuery(11), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ11, columnQ11);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ12()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ12 = new HashMap<String, List<String>>()
        {{
            put("orders", new ArrayList<String>() {{
                add("o_orderkey");
                add("o_orderpriority");
            }});
            put("lineitem", new ArrayList<String>() {{
                add("l_orderkey");
                add("l_shipmode");
                add("l_commitdate");
                add("l_receiptdate");
                add("l_shipdate");
            }});
        }};

        Map<String, List<String>> columnQ12 = deriveColumns(TpchQuery.getQuery(12), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ12, columnQ12);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ13()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ13 = new HashMap<String, List<String>>()
        {{
            put("customer", new ArrayList<String>() {{
                add("c_custkey");
            }});
            put("orders", new ArrayList<String>() {{
                add("o_orderkey");
                add("o_custkey");
                add("o_comment");
            }});
        }};

        Map<String, List<String>> columnQ13 = deriveColumns(TpchQuery.getQuery(13), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ13, columnQ13);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ14()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ14 = new HashMap<String, List<String>>()
        {{
            put("lineitem", new ArrayList<String>() {{
                add("l_partkey");
                add("l_extendedprice");
                add("l_discount");
                add("l_shipdate");
            }});
            put("part", new ArrayList<String>() {{
                add("p_partkey");
                add("p_type");
            }});
        }};

        Map<String, List<String>> columnQ14 = deriveColumns(TpchQuery.getQuery(14), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ14, columnQ14);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ15()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ15 = new HashMap<String, List<String>>()
        {{
            put("supplier", new ArrayList<String>() {{
                add("s_suppkey");
                add("s_name");
                add("s_address");
                add("s_phone");
            }});
            put("lineitem", new ArrayList<String>() {{
                add("l_suppkey");
                add("l_extendedprice");
                add("l_discount");
                add("l_shipdate");
            }});
        }};

        Map<String, List<String>> columnQ15 = deriveColumns(TpchQuery.getQuery(15), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ15, columnQ15);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ16()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ16 = new HashMap<String, List<String>>()
        {{
            put("partsupp", new ArrayList<String>() {{
                add("ps_partkey");
                add("ps_suppkey");
            }});
            put("part", new ArrayList<String>() {{
                add("p_partkey");
                add("p_brand");
                add("p_type");
                add("p_size");
            }});
            put("supplier", new ArrayList<String>() {{
                add("s_suppkey");
                add("s_comment");
            }});
        }};

        Map<String, List<String>> columnQ16 = deriveColumns(TpchQuery.getQuery(16), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ16, columnQ16);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ17()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ17 = new HashMap<String, List<String>>()
        {{
            put("lineitem", new ArrayList<String>() {{
                add("l_partkey");
                add("l_quantity");
                add("l_extendedprice");
            }});
            put("part", new ArrayList<String>() {{
                add("p_partkey");
                add("p_brand");
                add("p_container");
            }});
        }};

        Map<String, List<String>> columnQ17 = deriveColumns(TpchQuery.getQuery(17), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ17, columnQ17);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ18()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ18 = new HashMap<String, List<String>>()
        {{
            put("customer", new ArrayList<String>() {{
                add("c_name");
                add("c_custkey");
            }});
            put("orders", new ArrayList<String>() {{
                add("o_orderkey");
                add("o_orderdate");
                add("o_totalprice");
                add("o_custkey");
            }});
            put("lineitem", new ArrayList<String>() {{
                add("l_orderkey");
                add("l_quantity");
            }});
        }};

        Map<String, List<String>> columnQ18 = deriveColumns(TpchQuery.getQuery(18), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ18, columnQ18);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ19()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ19 = new HashMap<String, List<String>>()
        {{
            put("lineitem", new ArrayList<String>() {{
                add("l_partkey");
                add("l_quantity");
                add("l_extendedprice");
                add("l_discount");
                add("l_shipmode");
                add("l_shipinstruct");
            }});
            put("part", new ArrayList<String>() {{
                add("p_partkey");
                add("p_brand");
                add("p_container");
                add("p_size");
            }});
        }};

        Map<String, List<String>> columnQ19 = deriveColumns(TpchQuery.getQuery(19), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ19, columnQ19);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ20()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ20 = new HashMap<String, List<String>>()
        {{
            put("supplier", new ArrayList<String>() {{
                add("s_suppkey");
                add("s_name");
                add("s_address");
                add("s_nationkey");
            }});
            put("nation", new ArrayList<String>() {{
                add("n_nationkey");
                add("n_name");
            }});
            put("partsupp", new ArrayList<String>() {{
                add("ps_suppkey");
                add("ps_partkey");
                add("ps_availqty");
            }});
            put("lineitem", new ArrayList<String>() {{
                add("l_partkey");
                add("l_suppkey");
                add("l_quantity");
                add("l_shipdate");
            }});
            put("part", new ArrayList<String>() {{
                add("p_partkey");
                add("p_name");
            }});
        }};

        Map<String, List<String>> columnQ20 = deriveColumns(TpchQuery.getQuery(20), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ20, columnQ20);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ21()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ21 = new HashMap<String, List<String>>()
        {{
            put("supplier", new ArrayList<String>() {{
                add("s_suppkey");
                add("s_name");
                add("s_nationkey");
            }});
            put("lineitem", new ArrayList<String>() {{
                add("l_suppkey");
                add("l_orderkey");
                add("l_receiptdate");
                add("l_commitdate");
            }});
            put("orders", new ArrayList<String>() {{
                add("o_orderkey");
                add("o_orderstatus");
            }});
            put("nation", new ArrayList<String>() {{
                add("n_nationkey");
                add("n_name");
            }});
        }};

        Map<String, List<String>> columnQ21 = deriveColumns(TpchQuery.getQuery(21), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ21, columnQ21);
    }

    @Test
    public void testPlanAnalysisColumnTpchQ22()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        Map<String, List<String>> answerQ22 = new HashMap<String, List<String>>()
        {{
            put("customer", new ArrayList<String>() {{
                add("c_phone");
                add("c_acctbal");
                add("c_custkey");
            }});
            put("orders", new ArrayList<String>() {{
                add("o_custkey");
            }});
        }};

        Map<String, List<String>> columnQ22 = deriveColumns(TpchQuery.getQuery(22), "tpch");
        VALIDATE_COLUMN_CORRECTNESS(answerQ22, columnQ22);
    }

    @Test
    public void testPlanAnalysisColumnClickbenchExample()
            throws NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        String query = "SELECT watchid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM hits GROUP BY watchid, clientip ORDER BY c DESC LIMIT 10";
        Map<String, List<String>> answer = new HashMap<String, List<String>>()
        {{
            put("hits", new ArrayList<String>() {{
                add("watchid");
                add("clientip");
                add("isrefresh");
                add("resolutionwidth");
            }});
        }};

        Map<String, List<String>> columns = deriveColumns(query, "clickbench");
        VALIDATE_COLUMN_CORRECTNESS(answer, columns);
    }

    private Map<String, List<String>> deriveColumns(String query, String schema)
            throws SqlParseException, AmphiException, IOException, InterruptedException, MetadataException
    {
        PixelsParser deriveParser = null;
        if (schema.equals("tpch")) {
            deriveParser = this.tpchPixelsParser;
        } else if (schema.equals("clickbench")) {
            deriveParser = this.clickbenchPixelsParser;
        } else {
            throw new IllegalArgumentException("The schema name has not been registered as a parser: " + schema);
        }

        SqlNode parsedNode = deriveParser.parseQuery(query);
        SqlNode validatedNode = deriveParser.validate(parsedNode);
        RelNode rel = deriveParser.toRelNode(validatedNode);

        PlanAnalysis analysis = new PlanAnalysis(instance, query, rel, schema);
        analysis.analyze();
        return analysis.getProjectColumns();
    }

    private static void VALIDATE_COLUMN_CORRECTNESS(Map<String, List<String>> answer, Map<String, List<String>> result)
    {
        for (Map.Entry<String, List<String>> entry : result.entrySet())
        {
            String key = entry.getKey();
            List<String> resultList = entry.getValue();
            if (!resultList.isEmpty())
            {
                assertTrue(answer.containsKey(key));
                List<String> answerList = answer.get(key);
                assertEquals(new HashSet<>(answerList), new HashSet<>(resultList));
            }
        }
    }
}

