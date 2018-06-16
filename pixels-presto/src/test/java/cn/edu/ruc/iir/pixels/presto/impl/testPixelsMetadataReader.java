package cn.edu.ruc.iir.pixels.presto.impl;

import cn.edu.ruc.iir.pixels.presto.PixelsColumnHandle;
import cn.edu.ruc.iir.pixels.presto.PixelsTable;
import cn.edu.ruc.iir.pixels.presto.client.MetadataService;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.impl
 * @ClassName: testPixelsMetadataReader
 * @Description:
 * @author: tao
 * @date: Create in 2018-01-27 11:15
 **/
public class testPixelsMetadataReader {
    private PixelsMetadataReader pixelsMetadataReader = null;
    private final Logger log = Logger.getLogger(testPixelsMetadataReader.class.getName());

    @Before
    public void init ()
    {
        PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
        this.pixelsMetadataReader = new PixelsMetadataReader(new MetadataService(config));
    }

    @Test
    public void testGetSchemaNames() {
        List<String> schemaList = pixelsMetadataReader.getSchemaNames();
        System.out.println(schemaList.toString());
        log.info("Size: " + schemaList.size());
    }

    @Test
    public void testGetTableNames() {
        List<String> tablelist = pixelsMetadataReader.getTableNames("");
        System.out.println(tablelist.toString());
    }


    @Test
    public void testGetTableColumns () {
        List<PixelsColumnHandle> columnHandleList = pixelsMetadataReader.getTableColumn("", "pixels", "test30g_pixels");
        System.out.println(columnHandleList.toString());
    }

    @Test
    public void testLog() {
        log.info("Hello World");
    }

    @Test
    public void getTable() {
        PixelsTable table = pixelsMetadataReader.getTable("pixels", "default", "test");
        System.out.println(table.getTableHandle().toString());
        System.out.println(table.getTableLayout().toString());
        System.out.println(table.getColumns().toString());
    }
}
