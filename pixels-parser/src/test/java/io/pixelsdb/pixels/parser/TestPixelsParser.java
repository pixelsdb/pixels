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
package io.pixelsdb.pixels.parser;

import io.pixelsdb.pixels.common.metadata.MetadataService;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

import java.util.Properties;

public class TestPixelsParser
{
    String hostAddr = "ec2-18-216-64-72.us-east-2.compute.amazonaws.com";

    MetadataService instance = null;

    PixelsParser tpchPixelsParser = null;

    PixelsParser clickbenchPixelsParser = null;

    @Before
    public void init()
    {
        this.instance = new MetadataService(hostAddr, 18888);
        SqlParser.Config tpchParserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL_ANSI)
                .setParserFactory(SqlParserImpl.FACTORY)
                .build();

        SqlParser.Config clickbenchParserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL)
                .setConformance(SqlConformanceEnum.MYSQL_5)
                .setParserFactory(SqlParserImpl.FACTORY)
                .build();

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

        this.tpchPixelsParser = new PixelsParser(this.instance, "tpch", tpchParserConfig, properties);
        this.clickbenchPixelsParser = new PixelsParser(this.instance, "clickbench", clickbenchParserConfig, properties);
    }

    @After
    public void shutdown() throws InterruptedException
    {
        this.instance.shutdown();
    }

    @Test
    public void testPixelsParserTpchExample() throws SqlParseException
    {
        String query = TpchQuery.Q2;
        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(query);
        System.out.println("Parsed SQL Query: \n" + parsedNode);

        SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);
        System.out.println("No exception, validation success.");

        RelNode initialPlan = this.tpchPixelsParser.toRelNode(validatedNode);
        RelNode optimizedPlan = this.tpchPixelsParser.toBestRelNode(validatedNode);

        RelMetadataQuery mq = optimizedPlan.getCluster().getMetadataQuery();
        RelOptCost costInitial = mq.getCumulativeCost(initialPlan);
        RelOptCost costOptimized = mq.getCumulativeCost(optimizedPlan);
        System.out.println("Initial cost: " + costInitial + " | Optimized cost: " + costOptimized);

        assertTrue(costOptimized.isLe(costInitial));
    }

    @Test
    public void testPixelsParserTpchCoverage() throws SqlParseException, NoSuchFieldException, IllegalAccessException
    {
        for (int i = 1; i <= 22; i++) {
            String query = TpchQuery.getQuery(i);
            SqlNode parsedNode = this.tpchPixelsParser.parseQuery(query);
            System.out.println("Tpch Query " + i + ": \n" + parsedNode);

            SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);

            RelNode initialPlan = this.tpchPixelsParser.toRelNode(validatedNode);
            RelNode optimizedPlan = this.tpchPixelsParser.toBestRelNode(validatedNode);

            // not assert that optimized cost less than initial cost
            RelMetadataQuery mq = optimizedPlan.getCluster().getMetadataQuery();
            RelOptCost costInitial = mq.getCumulativeCost(initialPlan);
            RelOptCost costOptimized = mq.getCumulativeCost(optimizedPlan);
            System.out.println("Initial cost: " + costInitial + " | Optimized cost: " + costOptimized);
        }
    }

    @Test
    public void testPixelsParserClickbenchCoverage() throws SqlParseException, NoSuchFieldException, IllegalAccessException
    {
        for (int i = 30; i <= 43; i++) {
            if (i == 28 || i == 29 || i == 43) {
                System.out.println("Syntax not supported by Calcite");
                continue;
            }
            String query = ClickbenchQuery.getQuery(i);
            SqlNode parsedNode = this.clickbenchPixelsParser.parseQuery(query);
            System.out.println("Clickbench Query " + i + ": \n" + parsedNode);

            SqlNode validatedNode = this.clickbenchPixelsParser.validate(parsedNode);

            RelNode initialPlan = this.clickbenchPixelsParser.toRelNode(validatedNode);
            RelNode optimizedPlan = this.clickbenchPixelsParser.toBestRelNode(validatedNode);

            // not assert that optimized cost less than initial cost
            RelMetadataQuery mq = optimizedPlan.getCluster().getMetadataQuery();
            RelOptCost costInitial = mq.getCumulativeCost(initialPlan);
            RelOptCost costOptimized = mq.getCumulativeCost(optimizedPlan);
            System.out.println("Initial cost: " + costInitial + " | Optimized cost: " + costOptimized);
        }
    }

    @Test(expected = SqlParseException.class)
    public void testParserInvalidSyntaxFailure() throws SqlParseException
    {
        String invalidSyntaxQuery = "select * from CUSTOMER AND";
        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(invalidSyntaxQuery);
    }

    @Test(expected = CalciteContextException.class)
    public void testValidatorNonExistentColumnFailure() throws SqlParseException
    {
        String wrongColumnQuery = "select s_name from LINEITEM";
        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(wrongColumnQuery);
        SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);
    }

    @Test(expected = CalciteContextException.class)
    public void testValidatorNonExistentTableFailure() throws SqlParseException
    {
        String wrongTableQuery = "select * from VOIDTABLE";
        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(wrongTableQuery);
        SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);
    }
}
