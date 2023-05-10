package io.pixelsdb.pixels.parser;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestPixelsSchema
{
    String hostAddr = "ec2-18-218-128-203.us-east-2.compute.amazonaws.com";

    MetadataService instance = null;

    @Before
    public void init()
    {
        this.instance = new MetadataService(hostAddr, 18888);
    }

    @After
    public void shutdown() throws InterruptedException
    {
        this.instance.shutdown();
    }

    @Test
    public void testPixelsSchemaType() throws MetadataException
    {
        String schemaName = "pixels";
        PixelsSchema pixelsSchema = new PixelsSchema(schemaName, this.instance);
        Map<String, org.apache.calcite.schema.Table> tableMap = pixelsSchema.getTableMap();

        assertNotNull(tableMap);
        assertEquals("Pixels metadata should have 3 tables.", 3, tableMap.size());
        assertTrue(tableMap.containsKey("test_1187"));
        assertTrue(tableMap.containsKey("test_105"));
        assertTrue(tableMap.containsKey("test_date"));

        PixelsTable pixelsTable = (PixelsTable) tableMap.get("test_105");
        RelDataType rowType = pixelsTable.getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        List<RelDataTypeField> fieldList = rowType.getFieldList();

        assertEquals("Table test_105 should have 105 columns", 105, rowType.getFieldCount());
        assertEquals("querydayname", fieldList.get(1).getName());
        assertEquals(SqlTypeName.VARCHAR, fieldList.get(1).getType().getSqlTypeName());
        assertEquals("ismarketingtraffic", fieldList.get(3).getName());
        assertEquals(SqlTypeName.BOOLEAN, fieldList.get(3).getType().getSqlTypeName());
        assertEquals("queryviewcountwithanswerclicks", fieldList.get(11).getName());
        assertEquals(SqlTypeName.BIGINT, fieldList.get(11).getType().getSqlTypeName());
        assertEquals("requesttimeutcminute", fieldList.get(13).getName());
        assertEquals(SqlTypeName.TIMESTAMP, fieldList.get(13).getType().getSqlTypeName());
    }

    @Test
    public void testTpchSchemaType() throws MetadataException
    {
        String schemaName = "tpch";
        PixelsSchema tpchSchema = new PixelsSchema(schemaName, this.instance);
        Map<String, org.apache.calcite.schema.Table> tableMap = tpchSchema.getTableMap();

        Set<String> expectedTableNames = new HashSet<>(
                Arrays.asList("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")
        );

        assertNotNull(tableMap);
        assertEquals("TPC-H schema should have 8 tables.", 8, tableMap.size());
        assertEquals("Table names are correct.", expectedTableNames, tableMap.keySet());

        PixelsTable customerTable = (PixelsTable) tableMap.get("customer");
        RelDataType rowType = customerTable.getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        List<RelDataTypeField> fieldList = rowType.getFieldList();

        assertEquals("Table customer should have 8 columns", 8, rowType.getFieldCount());
        assertEquals("c_name", fieldList.get(1).getName());
        assertEquals(SqlTypeName.VARCHAR, fieldList.get(1).getType().getSqlTypeName());
        assertEquals("c_phone", fieldList.get(4).getName());
        assertEquals(SqlTypeName.CHAR, fieldList.get(4).getType().getSqlTypeName());
        assertEquals("c_acctbal", fieldList.get(5).getName());
        assertEquals(SqlTypeName.DECIMAL, fieldList.get(5).getType().getSqlTypeName());
    }
}
