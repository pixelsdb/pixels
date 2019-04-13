package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.common.metadata.domain.*;
import org.junit.Test;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        String tableName = "test_1187";

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

    @Test
    public void checkCompactLayout ()
    {
        String schemaName = "pixels";
        String tableName = "test_1187";

        SchemaDao schemaDao = new SchemaDao();
        TableDao tableDao = new TableDao();
        ColumnDao columnDao = new ColumnDao();
        LayoutDao layoutDao = new LayoutDao();

        Schema schema = schemaDao.getByName(schemaName);
        Table table = tableDao.getByNameAndSchema(tableName, schema);
        columnDao.getByTable(table);
        layoutDao.getByTable(table);


        Layout layout = null;

        for (Layout layout1 : table.getLayouts())
        {
            if (layout1.getId() == 14)
            {
                layout = layout1;
                break;
            }
        }

        List<String> columnOrder = layout.getOrderObject().getColumnOrder();
        int cacheBorder = layout.getCompactObject().getCacheBorder();
        List<String> columnletOrder = layout.getCompactObject().getColumnletOrder();
        Set<String> cachedColumns = new HashSet<>();
        for (int i = 0; i < cacheBorder; ++i)
        {
            int columnId = Integer.parseInt(columnletOrder.get(i).split(":")[1]);
            cachedColumns.add(columnOrder.get(columnId));
        }

        for (String column : cachedColumns)
        {
            System.out.println(column);
        }
    }

    // get from dbiir27
    @Test
    public void getLayout()
            throws IOException
    {
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File("/Users/Jelly/Desktop/dbiir10-splits")));
        LayoutDao layoutDao = new LayoutDao();
        Layout layout = layoutDao.getById(21);
        Order order = layout.getOrderObject();
        List<String> columnOrder = order.getColumnOrder();
        for (String col : columnOrder)
        {
            writer.write(col);
            writer.newLine();
        }
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
