package cn.edu.ruc.iir.pixels.presto.impl;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.presto.client.MetadataService;
import cn.edu.ruc.iir.pixels.presto.exception.PixelsUriExceotion;
import org.junit.Test;

import java.util.List;

public class TestMetadataService
{
    @Test
    public void testGetColumnsBySchemaNameAndTblName () throws PixelsUriExceotion
    {
        PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
        List<Column> columns = new MetadataService(config).getColumnsBySchemaNameAndTblName("pixels", "test");
        for (Column column : columns)
        {
            System.out.println(column.getColName() + ", " + column.getColType());
        }
    }
}
