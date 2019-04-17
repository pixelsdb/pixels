package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.common.metadata.domain.*;
import cn.edu.ruc.iir.pixels.daemon.MetadataProto;
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
        PbSchemaDao schemaDao = new PbSchemaDao();
        MetadataProto.Schema schema = schemaDao.getByName("pixels");
        System.out.println(schema.getId() + ", " + schema.getName() + ", " + schema.getDesc());
    }

    @Test
    public void testTable ()
    {
        PbTableDao tableDao = new PbTableDao();
        List<MetadataProto.Table> tables = tableDao.getByName("test_105");
        for (MetadataProto.Table table : tables)
        {
            System.out.println(table.getId() + ", " + table.getSchemaId());
        }
    }

    @Test
    public void testLayout ()
    {
        String schemaName = "pixels";
        String tableName = "test_1187";

        PbSchemaDao schemaDao = new PbSchemaDao();
        PbTableDao tableDao = new PbTableDao();
        PbColumnDao columnDao = new PbColumnDao();
        PbLayoutDao layoutDao = new PbLayoutDao();

        MetadataProto.Schema schema = schemaDao.getByName(schemaName);
        MetadataProto.Table table = tableDao.getByNameAndSchema(tableName, schema);
        List<MetadataProto.Column> columns = columnDao.getByTable(table);
        List<MetadataProto.Layout> layouts = layoutDao.getByTable(table);

        for (MetadataProto.Column column : columns)
        {
            System.out.println(column.getName() + ", " + column.getType());
        }

        for (MetadataProto.Layout layout : layouts)
        {
            System.out.println(layout.getOrderPath());
        }
    }

    @Test
    public void checkCompactLayout ()
    {
        String schemaName = "pixels";
        String tableName = "test_1187";

        PbSchemaDao schemaDao = new PbSchemaDao();
        PbTableDao tableDao = new PbTableDao();
        PbColumnDao columnDao = new PbColumnDao();
        PbLayoutDao layoutDao = new PbLayoutDao();

        MetadataProto.Schema schema = schemaDao.getByName(schemaName);
        MetadataProto.Table table = tableDao.getByNameAndSchema(tableName, schema);
        columnDao.getByTable(table);
        List<MetadataProto.Layout> layouts = layoutDao.getByTable(table);


        MetadataProto.Layout layout = null;

        for (MetadataProto.Layout layout1 : layouts)
        {
            if (layout1.getId() == 14)
            {
                layout = layout1;
                break;
            }
        }

        LayoutWrapper layoutWrapper = new LayoutWrapper(layout);
        List<String> columnOrder = layoutWrapper.getOrderObject().getColumnOrder();
        int cacheBorder = layoutWrapper.getCompactObject().getCacheBorder();
        List<String> columnletOrder = layoutWrapper.getCompactObject().getColumnletOrder();
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
        PbLayoutDao layoutDao = new PbLayoutDao();
        LayoutWrapper layout = new LayoutWrapper(layoutDao.getById(21));
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
        PbLayoutDao layoutDao = new PbLayoutDao();
        layoutDao.update(layoutDao.getById(10).toBuilder().setSplits(splits).build());
        reader.close();
    }
}
