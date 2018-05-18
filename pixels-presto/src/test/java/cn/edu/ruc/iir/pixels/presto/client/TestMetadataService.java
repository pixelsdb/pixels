package cn.edu.ruc.iir.pixels.presto.client;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.presto.exception.PixelsUriExceotion;
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
    public void init () throws PixelsUriExceotion
    {
        PixelsPrestoConfig config = new PixelsPrestoConfig().setMetadataServerUri("pixels://presto00:18888");
        this.instance = new MetadataService(config);
    }

    @Test
    public void testGetSchemaNames() {
        for (int i = 1; i <= 5; i++) {
            Thread t = new Thread(() -> {
                List<Schema> schemas = instance.getSchemas();
                System.out.println("Thread: " + schemas.size());
            });
            t.start();
        }

        List<Schema> schemas = instance.getSchemas();
        System.out.println("Command: " + schemas.size());
    }

    @Test
    public void testGetColumnsBySchemaNameAndTblName() {
        String schemaName = "pixels";
//        String tableName = "test";
        String tableName = "test30g_pixels";
//        for (int i = 1; i <= 20; i++) {
//            Thread t = new Thread(() -> {
//                List<Column> columns = instance.getColumnsBySchemaNameAndTblName(schemaName, tableName);
//                if (columns.size() != 0)
//                    System.out.println("Thread: " + columns.size());
//            });
//            t.start();
//        }
        List<Column> columns = instance.getColumnsBySchemaNameAndTblName(schemaName, tableName);
        System.out.println("Command: " + columns.size());
    }
}
