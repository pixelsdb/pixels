package cn.edu.ruc.iir.pixels.presto.impl;

import cn.edu.ruc.iir.pixels.presto.PixelsTable;
import cn.edu.ruc.iir.pixels.presto.PixelsTableLayoutHandle;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.impl
 * @ClassName: PixelsMetadataReader
 * @Description: Read metadata
 * @author: tao
 * @date: Create in 2018-01-20 11:15
 **/
public class PixelsMetadataReader {

    private static final Logger log = Logger.get(PixelsMetadataReader.class);


    public List<String> getSchemaNames() {
        List<String> schemaList = new ArrayList<String>();
        schemaList.add("default");
        schemaList.add("schema1");
        schemaList.add("schema2");
        schemaList.add("schemaN");
        return schemaList;
    }

    public Set<String> getTableNames(String schemaName) {
        Set<String> tablelist = new HashSet<String>(){{
            add("tab1");
            add("tab2");
            add("tabN");
        }};
        return tablelist;
    }

    public static PixelsTable getTable(String connectorId, String schemaName, String tableName) {
        PixelsTable table = null;

        return table;
    }

    public static PixelsTableLayoutHandle getTableLayout(String connectorId, String schemaName, String tableName) {
        PixelsTableLayoutHandle tableHandle = null;

        return tableHandle;
    }
}
