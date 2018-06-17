package cn.edu.ruc.iir.pixels.presto.client;

import cn.edu.ruc.iir.pixels.common.metadata.Column;
import cn.edu.ruc.iir.pixels.common.metadata.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.Schema;
import cn.edu.ruc.iir.pixels.presto.impl.PixelsPrestoConfig;
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
        PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
        this.instance = new MetadataService(config);
    }

    @Test
    public void testGetSchemaNames() throws InterruptedException
    {
        for (int i = 1; i <= 5; i++) {
            Thread t = new Thread(() -> {
                List<Schema> schemas = instance.getSchemas();
                System.out.println("Thread: " + schemas.size());
            });
            t.start();
            t.join();
        }

        List<Schema> schemas = instance.getSchemas();
        System.out.println("Command: " + schemas.size());
    }

    @Test
    public void testGetColumnsBySchemaNameAndTblName ()
    {
        List<Column> columns = instance.getColumns("pixels", "test");
        for (Column column : columns)
        {
            System.out.println(column.getName() + ", " + column.getType());
        }
    }

    @Test
    public void testGetTableLayouts () {
        List<Layout> layouts = instance.getLayouts("pixels", "test30g_pixels");
        System.out.println(layouts.get(0).getSplits());
    }
}
