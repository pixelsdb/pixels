package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.common.metadata.domain.Base;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Table;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class TestDaos
{
    @Test
    public void testSchema ()
    {
        SchemaDao schemaDao = new SchemaDao();
        Schema schema = schemaDao.getByName("pixels");
        System.out.println(schema.getId() + ", " + schema.getName() + ", " + schema.getDesc());
    }

    @Test
    public void testTable ()
    {
        TableDao tableDao = new TableDao();
        List<Table> tables = tableDao.getByName("test_105");
        for (Table table : tables)
        {
            System.out.println(table.getId() + ", " + table.getSchema().getName());
            Base base = table;
            System.out.println(base.toString());
        }
    }

    @Test
    public void testLayout ()
    {
        String schemaName = "pixels";
        String tableName = "test_105";

        SchemaDao schemaDao = new SchemaDao();
        TableDao tableDao = new TableDao();
        ColumnDao columnDao = new ColumnDao();
        LayoutDao layoutDao = new LayoutDao();

        Schema schema = schemaDao.getByName(schemaName);
        Table table = tableDao.getByNameAndSchema(tableName, schema);
        columnDao.getByTable(table);
        layoutDao.getByTable(table);

        for (Column column : table.getColumns())
        {
            System.out.println(column.getName() + ", " + column.getType());
        }

        for (Layout layout : table.getLayouts())
        {
            System.out.println(layout.getOrderPath());
        }
    }

    // get from dbiir27
    @Test
    public void getLayout()
            throws IOException
    {
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File("/Users/Jelly/Desktop/dbiir10-splits")));
        LayoutDao layoutDao = new LayoutDao();
        Layout layout = layoutDao.getById(10);
        String splits = layout.getSplits();
        writer.write(splits);
        writer.flush();
        writer.close();
    }

    // update dbiir10
    @Test
    public void updateLayout()
            throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader(new File("/Users/Jelly/Desktop/splits")));
        String splits = reader.readLine();
        LayoutDao layoutDao = new LayoutDao();
        Layout layout = layoutDao.getById(10);
        layout.setSplits(splits);
        layoutDao.update(layout);
        reader.close();
    }
}
