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
package io.pixelsdb.pixels.amphi.downloader;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestPartialSchemaLoader
{
    private static final String RESOURCE_PATH = "src/main/resources";
    private static final PartialSchemaLoader partialSchemaLoader = new PartialSchemaLoader();

    @Before
    public void setup() throws IOException
    {
        partialSchemaLoader.registerAllSchemas(RESOURCE_PATH);
    }

    @Test
    public void testEmptyFields()
    {
        List<String> emptyField = new ArrayList<>();
        Schema emptyScheam = partialSchemaLoader.getPartialSchema("region", emptyField);
        assertTrue("Partial schema is not empty.", emptyScheam.getFields().isEmpty());
    }

    @Test
    public void testPartialLineitem()
    {
        List<String> fields = Arrays.asList("l_orderkey", "l_quantity", "l_linestatus", "l_shipdate");
        Schema partialSchema = partialSchemaLoader.getPartialSchema("lineitem", fields);

        assertNotNull(partialSchema);
        assertEquals("Partial schema does not have expected number of fields.",
                fields.size(), partialSchema.getFields().size());
        assertEquals(Schema.Type.LONG, partialSchema.getField("l_orderkey").schema().getType());
        assertEquals(Schema.Type.BYTES, partialSchema.getField("l_quantity").schema().getType());
        assertEquals("decimal", partialSchema.getField("l_quantity").schema().getLogicalType().getName());
        assertEquals(Schema.Type.STRING, partialSchema.getField("l_linestatus").schema().getType());
        assertEquals(Schema.Type.INT, partialSchema.getField("l_shipdate").schema().getType());
        assertEquals("date", partialSchema.getField("l_shipdate").schema().getLogicalType().getName());
    }

    @Test
    public void testNonexistentSchema()
    {
        List<String> fields = Arrays.asList("r_name");
        assertThrows("PartialSchemaLoader does not throw exception for loading non existent schema",
                IllegalArgumentException.class,
                () -> partialSchemaLoader.getPartialSchema("null", fields)
        );
    }

    @Test
    public void testNonexistentField()
    {
        List<String> fields = Arrays.asList("r_name", "r_null");
        assertThrows("PartialSchemaLoader does not throw exception for loading non existent fields",
                IllegalArgumentException.class,
                () -> partialSchemaLoader.getPartialSchema("region", fields)
        );
    }
}
