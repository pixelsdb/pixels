/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
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

import java.util.*;

import static org.junit.Assert.*;

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
        assertEquals("requesttimeutcminute", fieldList.get(13).getName());
        assertEquals(SqlTypeName.TIMESTAMP, fieldList.get(13).getType().getSqlTypeName());

        // TIMESTAMP(3)
        assertEquals(3, fieldList.get(13).getValue().getPrecision());
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

        // DECIMAL(15, 2)
        assertEquals(15, fieldList.get(5).getValue().getPrecision());
        assertEquals(2, fieldList.get(5).getValue().getScale());
    }
}
