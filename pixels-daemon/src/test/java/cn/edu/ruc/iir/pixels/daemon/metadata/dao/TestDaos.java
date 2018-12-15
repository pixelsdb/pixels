package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.common.metadata.domain.*;
import org.junit.Test;

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
}
