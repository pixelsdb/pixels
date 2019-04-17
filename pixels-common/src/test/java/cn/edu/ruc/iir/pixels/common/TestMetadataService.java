package cn.edu.ruc.iir.pixels.common;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.domain.*;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.daemon.MetadataProto;
import com.alibaba.fastjson.JSON;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.client
 * @ClassName: TestMetadataService
 * @Description:
 * @author: tao
 * @date: Create in 2018-01-30 15:36
 **/
public class TestMetadataService {
    MetadataService instance = null;

    @Before
    public void init ()
    {
        this.instance = new MetadataService("dbiir10", 18888);
    }

    @Test
    public void testGetSchemaNames() throws InterruptedException, MetadataException
    {
        for (int i = 1; i <= 5; i++) {
            Thread t = new Thread(() -> {
                List<MetadataProto.Schema> schemas = null;
                try
                {
                    schemas = instance.getSchemas();
                } catch (MetadataException e)
                {
                    e.printStackTrace();
                }
                System.out.println("Thread: " + schemas.size());
            });
            t.start();
            t.join();
        }

        List<MetadataProto.Schema> schemas = instance.getSchemas();
        System.out.println("Command: " + schemas.size());
    }

    @Test
    public void testGetTableNames () throws MetadataException
    {
        String schemaName = "pixels";
        List<String> tableList = new ArrayList<String>();
        List<MetadataProto.Table> tables = instance.getTables(schemaName);
        for (MetadataProto.Table t : tables) {
            tableList.add(t.getName());
        }
        System.out.println("Show tables, " + tableList.toString());
    }

    @Test
    public void testGetColumnsBySchemaNameAndTblName () throws MetadataException
    {
        List<MetadataProto.Column> columns = instance.getColumns("pixels", "test_105");
        for (MetadataProto.Column column : columns)
        {
            System.out.println(column.getName() + ", " + column.getType());
        }
    }

    @Test
    public void testGetTableLayouts () throws MetadataException
    {
        long start = System.currentTimeMillis();
        List<MetadataProto.Layout> layouts = instance.getLayouts("pixels", "test_105");
        long end = System.currentTimeMillis();
        System.out.println("Last: " + (end - start));
        System.out.println(layouts.get(0).getSplits());

        for (MetadataProto.Layout layout : layouts) {
            // get index
            int version = layout.getVersion();
            Order order = JSON.parseObject(layout.getOrder(), Order.class);
            Splits splits = JSON.parseObject(layout.getSplits(), Splits.class);
            System.out.println(order.toString());
            System.out.println(splits.toString());
        }

    }


    @Test
    public void testGetTableLayoutsByVersion () throws MetadataException
    {
        long start = System.currentTimeMillis();
        MetadataProto.Layout layout = instance.getLayout("pixels", "test_105", 0);
        long end = System.currentTimeMillis();
        System.out.println("Last: " + (end - start));
        System.out.println(layout.getSplits());
    }
}
