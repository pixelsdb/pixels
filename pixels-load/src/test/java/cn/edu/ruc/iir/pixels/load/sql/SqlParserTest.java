package cn.edu.ruc.iir.pixels.load.sql;

import cn.edu.ruc.iir.pixels.common.utils.FileUtil;
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
