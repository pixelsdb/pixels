package cn.edu.ruc.iir.pixels.presto.impl;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.presto.client.MetadataService;
import org.junit.Test;

import java.util.List;

public class TestMetadataService
{
    @Test
    public void testGetColumnsBySchemaNameAndTblName ()
    {
        List<Column> columns = MetadataService.getColumnsBySchemaNameAndTblName("pixels", "test");
        for (Column column : columns)
        {
            System.out.println(column.getColName() + ", " + column.getColType());
        }
    }
}
