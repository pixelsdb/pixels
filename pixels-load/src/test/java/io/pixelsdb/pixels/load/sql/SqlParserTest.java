/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.load.sql;

import io.pixelsdb.pixels.common.utils.FileUtil;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SqlParserTest {
    private SqlParser parser = new SqlParser();

    @Test
    public void testCreateSchema() {
        List<Property> properties = new ArrayList<>();
        String sql = "CREATE SCHEMA IF NOT EXISTS test";
        Statement statement = parser.createStatement(sql);
        CreateSchema expected = new CreateSchema(QualifiedName.of("test"), true, properties);
        System.out.println(statement.toString());
        assertEquals(statement.toString(), expected.toString());
    }

    @Test
    public void testCreateTable() {
        String sql =
                "CREATE TABLE orders (\n" +
                        "  orderkey bigint,\n" +
                        "  orderstatus varchar,\n" +
                        "  totalprice double,\n" +
                        "  orderdate date\n" +
                        ")\n" +
                        "WITH (format = 'ORC')";
//        System.out.println("sql: " + sql);
//        Statement statement = parser.createStatement(sql);
//        System.out.println("statement: " + statement.toString());

        CreateTable createTable = (CreateTable) parser.createStatement(sql);
        System.out.println("table: " + createTable.getName().toString());

        List<TableElement> elements = createTable.getElements();
        for (TableElement e : elements) {
//            System.out.println(e.toString());
            ColumnDefinition column = (ColumnDefinition) e;
            System.out.println("column: " + column.toString());
            System.out.println("column: " + column.getName() + " " + column.getType());
        }
    }

    @Test
    public void testCreateTableFromTxt() {
        String schemaFile = "/home/tao/software/data/pixels/test30G_pixels/presto_ddl.sql";
        String sql = FileUtil.readFileToString(schemaFile);

        CreateTable createTable = (CreateTable) parser.createStatement(sql);
        System.out.println("table: " + createTable.getName().toString());

        List<TableElement> elements = createTable.getElements();
        for (TableElement e : elements) {
            ColumnDefinition column = (ColumnDefinition) e;
//            System.out.println("column: " + column.toString());
            System.out.println("column: " + column.getName() + " " + column.getType());
        }
    }

}
