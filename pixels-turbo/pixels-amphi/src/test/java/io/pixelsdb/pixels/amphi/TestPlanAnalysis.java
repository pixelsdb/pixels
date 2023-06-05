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
package io.pixelsdb.pixels.amphi;

import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.parser.PixelsParser;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class TestPlanAnalysis
{
    String hostAddr = "ec2-18-218-128-203.us-east-2.compute.amazonaws.com";

    MetadataService instance = null;

    PixelsParser tpchPixelsParser = null;

    @Before
    public void init()
    {
        this.instance = new MetadataService(hostAddr, 18888);
        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL_ANSI)
                .setParserFactory(SqlParserImpl.FACTORY)
                .build();

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

        this.tpchPixelsParser = new PixelsParser(this.instance, "tpch", parserConfig, properties);
    }

    @After
    public void shutdown() throws InterruptedException
    {
        this.instance.shutdown();
    }

    String query = "select o_year, sum( case when nation = 'INDIA' then volume else 0 end) / sum(volume) as mkt_share " +
            "from( select extract( year from o_orderdate ) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation " +
            "from PART, SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION n1, NATION n2, REGION " +
            "where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and " +
            "r_name = 'ASIA' and s_nationkey = n2.n_nationkey and o_orderdate between '1995-01-01' and '1996-12-31' and p_type = 'SMALL PLATED COPPER' ) as all_nations " +
            "group by o_year " +
            "order by o_year";

    @Test
    public void testPlanAnalysisExample() throws SqlParseException
    {
        String query = "select COUNT(*) from orders, customer";

        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(query);
        System.out.println("Parsed SQL Query: \n" + parsedNode);

        SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);
        System.out.println("No exception, validation success.");

        RelNode rel = this.tpchPixelsParser.toRelNode(validatedNode);
        final RelJsonWriter writer = new RelJsonWriter();
        rel.explain(writer);
        System.out.println("Logical plan: \n" + writer.asString());

        PlanAnalysis analysis = new PlanAnalysis(rel);
        analysis.traversePlan();

        Set<String> operatorTypes = analysis.getOperatorTypes();
        Set<String> scannedTables = analysis.getScannedTables();
        System.out.println("Logical operators in plan: " + operatorTypes);
        System.out.println("Scanned tables in plan: " + scannedTables);
        Set<String> expectedOperators = new HashSet<String>() {{
            add("LogicalAggregate");
            add("LogicalJoin");
            add("LogicalTableScan");
        }};
        Set<String> expectedScannedTables = new HashSet<String>() {{
            add("orders");
            add("customer");
        }};

        assertEquals("3 Logical operators included in plan.", expectedOperators, operatorTypes);
        assertEquals("2 tables scanned by the plan.", expectedScannedTables, scannedTables);
    }
}
