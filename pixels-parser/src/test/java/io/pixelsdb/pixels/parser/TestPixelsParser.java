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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class TestPixelsParser
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

    @Test
    public void testPixelsParserTpchExample() throws SqlParseException
    {
        String query = TpchQuery.Q8;
        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(query);
        System.out.println("Parsed SQL Query: \n" + parsedNode);

        SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);
        System.out.println("No exception, validation success.");

        RelNode rel = this.tpchPixelsParser.toRelNode(validatedNode);
        final RelJsonWriter writer = new RelJsonWriter();
        rel.explain(writer);
        System.out.println("Logical plan: \n" + writer.asString());
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
