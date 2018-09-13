package cn.edu.ruc.iir.pixels.presto.impl;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.presto.PixelsColumnHandle;
import cn.edu.ruc.iir.pixels.presto.PixelsTable;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.impl
 * @ClassName: testPixelsMetadataProxy
 * @Description:
 * @author: tao
 * @date: Create in 2018-01-27 11:15
 **/
public class testPixelsMetadataProxy
{
    private PixelsMetadataProxy pixelsMetadataProxy = null;
    private final Logger log = Logger.getLogger(testPixelsMetadataProxy.class.getName());

    @Before
    public void init ()
    {
        PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
        this.pixelsMetadataProxy = new PixelsMetadataProxy(config);
    }

    @Test
    public void testGetSchemaNames() throws MetadataException
    {
        List<String> schemaList = pixelsMetadataProxy.getSchemaNames();
        System.out.println(schemaList.toString());
        log.info("Size: " + schemaList.size());
    }

    @Test
    public void testGetTableNames() throws MetadataException
    {
        List<String> tablelist = pixelsMetadataProxy.getTableNames("pixels");
        System.out.println(tablelist.toString());
    }

    @Test
    public void testGetTableColumns () throws MetadataException
    {
        List<PixelsColumnHandle> columnHandleList = pixelsMetadataProxy.getTableColumn("", "pixels", "test30g_pixels");
        System.out.println(columnHandleList.toString());
    }

    @Test
    public void testLog() {
        log.info("Hello World");
    }

    @Test
    public void getTable() throws MetadataException
    {
        PixelsTable table = pixelsMetadataProxy.getTable("pixels", "default", "test");
        System.out.println(table.getTableHandle().toString());
        System.out.println(table.getTableLayout().toString());
        System.out.println(table.getColumns().toString());
    }
}
