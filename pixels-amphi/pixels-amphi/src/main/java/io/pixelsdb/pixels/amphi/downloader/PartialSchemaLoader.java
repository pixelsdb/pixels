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
import org.apache.avro.SchemaBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Partial Avro Schema, load and select subset of columns of the full schema
 *
 */
public class PartialSchemaLoader
{
    // Store all schemas pre-defined in the resources folder
    private static final Map<String, Schema> registeredSchemas = new HashMap<>();

    PartialSchemaLoader()
    {
    }

    public static void registerAllSchemas(String resourcePath) throws IOException
    {
        Files.walk(Paths.get(resourcePath))
                .filter(Files::isRegularFile)
                .forEach(file -> {
                    try {
                        Schema schema = new Schema.Parser().parse(new File(file.toString()));
                        registeredSchemas.put(schema.getFullName(), schema);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    // Specify a subset of fields to acquire partial schema
    public static Schema getPartialSchema(String schemaName, List<String> fields)
    {
        Schema fullSchema = registeredSchemas.get(schemaName);
        if (fullSchema == null)
        {
            throw new IllegalArgumentException("Schema " + schemaName + " does not exist.");
        }

        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(fullSchema.getName())
                .namespace(fullSchema.getNamespace())
                .fields();

        for (String fieldName : fields)
        {
            Schema.Field field = fullSchema.getField(fieldName);

            if (field == null)
            {
                throw new IllegalArgumentException("Field " + fieldName + " does not exist in schema " + schemaName);
            }

            fieldAssembler.name(field.name()).type(field.schema()).noDefault();
        }

        return fieldAssembler.endRecord();
    }
}
