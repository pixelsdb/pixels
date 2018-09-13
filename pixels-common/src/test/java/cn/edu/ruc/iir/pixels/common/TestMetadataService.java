package cn.edu.ruc.iir.pixels.common;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Schema;
import org.junit.Before;
import org.junit.Test;

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
        this.instance = new MetadataService("presto00", 18888);
    }

    @Test
    public void testGetSchemaNames() throws InterruptedException, MetadataException
    {
        for (int i = 1; i <= 5; i++) {
            Thread t = new Thread(() -> {
                List<Schema> schemas = null;
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

        List<Schema> schemas = instance.getSchemas();
        System.out.println("Command: " + schemas.size());
    }

    @Test
    public void testGetColumnsBySchemaNameAndTblName () throws MetadataException
    {
        List<Column> columns = instance.getColumns("pixels", "test");
        for (Column column : columns)
        {
            System.out.println(column.getName() + ", " + column.getType());
        }
    }

    @Test
    public void testGetTableLayouts () throws MetadataException
    {
        List<Layout> layouts = instance.getLayouts("pixels", "test30g_pixels");
        System.out.println(layouts.get(0).getSplits());
    }
}
