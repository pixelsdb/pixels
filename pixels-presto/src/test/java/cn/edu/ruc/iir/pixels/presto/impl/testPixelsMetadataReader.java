package cn.edu.ruc.iir.pixels.presto.impl;

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
    PixelsMetadataReader pixelsMetadataReader = new PixelsMetadataReader();

    @Test
    public void testGetSchemaNames() {
        List<String> schemaList  = pixelsMetadataReader.getSchemaNames();
        System.out.println(schemaList.toString());
    }

    @Test
    public void testGetTableNames() {
        List<String> tablelist  = pixelsMetadataReader.getTableNames("");
        System.out.println(tablelist.toString());
    }
}
